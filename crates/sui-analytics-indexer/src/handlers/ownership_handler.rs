use diesel::prelude::*;
use diesel::pg::PgConnection;
use diesel::result::Error;
use std::path::Path;
use sui_data_ingestion_core::Worker;
use sui_types::SYSTEM_PACKAGE_ADDRESSES;
use crate::schema::ownership;
// use sui_types::{Object, ObjectStatusTracker, State, OwnershipEntry};
use sui_types::object::Object;
use sui_types::effects::TransactionEffects;
use crate::tables::{OwnershipEntry, ObjectStatus};
use tokio::sync::Mutex;
use crate::handlers::{
    get_owner_address, get_owner_type, initial_shared_version, AnalyticsHandler,
    ObjectStatusTracker,
};
use sui_types::base_types::ObjectID;
use sui_package_resolver::Resolver;
use sui_rpc_api::{CheckpointData, CheckpointTransaction};
use crate::package_store::{LocalDBPackageStore, PackageCache};
pub struct PostgresHandler {
    connection: PgConnection,
}
use crate::FileType;
struct State {
    objects: Vec<OwnershipEntry>,
    package_store: LocalDBPackageStore,
    resolver: Resolver<PackageCache>,
}
use anyhow::Result;

pub struct OwnershipHandler {
    state: Mutex<State>,
    package_filter: Option<ObjectID>,
}
use tracing::{info, warn};
impl PostgresHandler {
    pub fn new(database_url: &str) -> Self {
        let connection = PgConnection::establish(database_url)
            .expect(&format!("Error connecting to {}", database_url));
        Self { connection }
    }

    pub fn insert_ownership_entries(&self, current_entry: &mut OwnershipEntry, previous_entry: Option<OwnershipEntry>) -> Result<(), Error> {
        use crate::schema::ownership::dsl::*;

        // Insert entry for the previous owner with balance 0 if ownership changed
        if let Some(prev_entry) = previous_entry {
            if prev_entry.owner_address != current_entry.owner_address {
                let transfer_out_entry = OwnershipEntry {
                    object_id: prev_entry.object_id.clone(),
                    version: prev_entry.version,
                    checkpoint: prev_entry.checkpoint,
                    epoch: prev_entry.epoch,
                    timestamp_ms: prev_entry.timestamp_ms,
                    owner_type: prev_entry.owner_type.clone(),
                    owner_address: prev_entry.owner_address.clone(),
                    object_status: "Transfer Out".to_string(),
                    previous_transaction: prev_entry.previous_transaction.clone(),
                    coin_type: prev_entry.coin_type.clone(),
                    coin_balance: 0,
                    previous_owner: prev_entry.previous_owner.clone(),
                    previous_version: prev_entry.previous_version,
                    previous_checkpoint: prev_entry.previous_checkpoint,
                    previous_coin_type: prev_entry.previous_coin_type.clone(),
                    previous_type: prev_entry.previous_type.clone(),
                };
                info!("Previous entry exists {:?}", prev_entry.object_id.clone());
                info!("current entry exists {:?}", current_entry.object_id);
                diesel::insert_into(ownership)
                    .values(&transfer_out_entry)
                    .execute(&self.connection)?;
                current_entry.object_status = "Transfer In".to_string();            
            } else {
                current_entry.object_status = "Mutated".to_string();
            }
        } else {
            current_entry.object_status = "Created".to_string();
        }

        // Insert entry for the current owner
        diesel::insert_into(ownership)
            .values(&*current_entry)
            .execute(&self.connection)?;

        Ok(())
    }

    pub fn get_previous_entry(&self, object_id: &str) -> Result<Option<OwnershipEntry>, Error> {
        use crate::schema::ownership::dsl::*;
        ownership
            .filter(object_id.eq(object_id))
            .order(version.desc())
            .first::<OwnershipEntry>(&self.connection)
            .optional()
    }


}

#[async_trait::async_trait]
impl Worker for OwnershipHandler {
    type Result = ();

    async fn process_checkpoint(&self, checkpoint_data: &CheckpointData) -> Result<()> {
        let CheckpointData {
            checkpoint_summary,
            transactions: checkpoint_transactions,
            ..
        } = checkpoint_data;
        let mut state = self.state.lock().await;

        for checkpoint_transaction in checkpoint_transactions {
            // for object in &checkpoint_transaction.output_objects {
            //     println!(
            //         "Output Object ID: {:?}, Balance: {:?}, Owner Address: {:?}",
            //         object.id(),
            //         if object.coin_type_maybe().is_some() {
            //             object.get_coin_value_unsafe().try_into().unwrap()
            //         } else {
            //             0
            //         },
            //         get_owner_address(object)
            //     );
            // }

            // Print input_objects and their object_ids, balances, and owner_addresses
            let object_status_tracker = ObjectStatusTracker::new(effects);
            for object in &checkpoint_transaction.all_removed_objects {
                println!(
                    "Input Object ID: {:?}, Owner Address: {:?}",
                    object.id(),
                    get_owner_address(object),
                );
            }
            for object in checkpoint_transaction.output_objects.iter() {
                state.package_store.update(object)?;
            }
            self.process_transaction(
                checkpoint_summary.epoch,
                checkpoint_summary.sequence_number,
                checkpoint_summary.timestamp_ms,
                checkpoint_transaction,
                &checkpoint_transaction.effects,
                &mut state,
            )
            .await?;
            if checkpoint_summary.end_of_epoch_data.is_some() {
                state
                    .resolver
                    .package_store()
                    .evict(SYSTEM_PACKAGE_ADDRESSES.iter().copied());
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AnalyticsHandler<OwnershipEntry> for OwnershipHandler {
    async fn read(&self) -> Result<Vec<OwnershipEntry>> {
        let mut state = self.state.lock().await;
        let cloned = state.objects.clone();
        state.objects.clear();
        Ok(cloned)
    }

    fn file_type(&self) -> Result<FileType> {
        Ok(FileType::Ownership)
    }

    fn name(&self) -> &str {
        "ownership"
    }
}


impl OwnershipHandler {
    pub fn new(store_path: &Path, rest_uri: &str, package_filter: &Option<String>) -> Self {
        let package_store = LocalDBPackageStore::new(&store_path.join("object"), rest_uri);
        
        let state = State {
            objects: vec![],
            package_store: package_store.clone(),
            resolver: Resolver::new(PackageCache::new(package_store)),
        };
        Self {
            state: Mutex::new(state),
            package_filter: package_filter
                .clone()
                .map(|x| ObjectID::from_hex_literal(&x).unwrap()),
        }
    }

    async fn process_transaction(
        &self,
        epoch: u64,
        checkpoint: u64,
        timestamp_ms: u64,
        checkpoint_transaction: &CheckpointTransaction,
        effects: &TransactionEffects,
        state: &mut State,
    ) -> Result<()> {
        let object_status_tracker = ObjectStatusTracker::new(effects);
        for object in checkpoint_transaction.output_objects.iter() {
            self.process_object(
                epoch,
                checkpoint,
                timestamp_ms,
                object,
                &object_status_tracker,
                state,
            )
            .await?;
        }
        for (object_ref, _) in effects.all_removed_objects().iter() {
                // get_owner_address(&object);
            println!("Deleted Object Ref: {:?}", object_ref);
            // info!(
            //     "Deleted Object ID: {:?}, Version: {:?}, Owner Address: {:?}",
            //     object_ref.0,
            //     object_ref.1.value(),
            //     // get_owner_address(&object);
            // );
            let entry = OwnershipEntry {
                object_id: object_ref.0.to_string(),
                version: object_ref.1.value().try_into().unwrap(),
                checkpoint: checkpoint.try_into().unwrap(),
                epoch: epoch.try_into().unwrap(),
                timestamp_ms: timestamp_ms.try_into().unwrap(),
                owner_type: None,
                owner_address: None,
                object_status: "Deleted".to_string(),
                previous_transaction: checkpoint_transaction.transaction.digest().base58_encode(),
                coin_type: None,
                coin_balance: 0,
                previous_owner: None,
                previous_version: None,
                previous_checkpoint: None,
                previous_coin_type: None,
                previous_type: None,
            };
            state.objects.push(entry);
        }
        Ok(())
    }

    async fn process_object(
        &self,
        epoch: u64,
        checkpoint: u64,
        timestamp_ms: u64,
        object: &Object,
        object_status_tracker: &ObjectStatusTracker,
        state: &mut State,
    ) -> Result<()> {
        let move_obj_opt = object.data.try_as_move();
        let has_public_transfer = move_obj_opt
            .map(|o| o.has_public_transfer())
            .unwrap_or(false);

        state.package_store.update(object)?;

        let object_type = move_obj_opt.map(|o| o.type_());
        let is_match = if let Some(package_id) = self.package_filter {
            if let Some(move_object_type) = object_type {
                let object_package_id: ObjectID = move_object_type.address().into();
                object_package_id == package_id
            } else {
                false
            }
        } else {
            true
        };

        if !is_match {
            return Ok(());
        }
        let object_id = object.id();
        let status = object_status_tracker.get_object_status(&object_id);

        if object_type.map(|t| t.to_string()) == Some("0x2::coin::Coin<0x2::sui::SUI>".to_string()) {
            let database_url = "postgres://davidyun:postgres@localhost/sui_indexer";
            let postgres_handler = PostgresHandler::new(&database_url);
            let mut current_entry = OwnershipEntry {
                object_id: object_id.to_string(),
                version: object.version().value().try_into().unwrap(),
                checkpoint: checkpoint.try_into().unwrap(),
                epoch: epoch.try_into().unwrap(),
                timestamp_ms: timestamp_ms.try_into().unwrap(),
                owner_type: Some(get_owner_type(object)).map(|ot| ot.to_string()),
                owner_address: get_owner_address(object),
                object_status: object_status_tracker
                    .get_object_status(&object_id)
                    .expect("Object must be in output objects")
                    .to_string(),
                previous_transaction: object.previous_transaction.base58_encode(),
                coin_type: object.coin_type_maybe().map(|t| t.to_string()),
                coin_balance: if object.coin_type_maybe().is_some() {
                    object.get_coin_value_unsafe().try_into().unwrap()
                } else {
                    0
                },
                previous_owner: None,
                previous_version: None,
                previous_checkpoint: None,
                previous_coin_type: None,
                previous_type: None,
            };

        if current_entry.object_status == "Mutated" {
            // Get the previous entry for the object
            let previous_entry = postgres_handler.get_previous_entry(&object_id.to_string())?;

            // Insert ownership entries
            postgres_handler.insert_ownership_entries(&mut current_entry, previous_entry)?;
        } else if current_entry.object_status == "Created" {
            // Insert ownership entry without previous entry logic
            postgres_handler.insert_ownership_entries(&mut current_entry, None)?;
        }
            state.objects.push(current_entry);
        }
        Ok(())
    }
}