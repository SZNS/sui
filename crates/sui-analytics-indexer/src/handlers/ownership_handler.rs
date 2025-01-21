use anyhow::Result;
use std::path::Path;
use sui_data_ingestion_core::Worker;
use sui_types::SYSTEM_PACKAGE_ADDRESSES;
use tokio::sync::Mutex;

use sui_package_resolver::Resolver;
use sui_rpc_api::{CheckpointData, CheckpointTransaction};
use sui_types::base_types::ObjectID;
use sui_types::effects::TransactionEffects;
use sui_types::object::Object;

use crate::handlers::{get_owner_address, get_owner_type, AnalyticsHandler, ObjectStatusTracker};
use crate::package_store::{LocalDBPackageStore, PackageCache};
use crate::tables::OwnershipEntry;
use crate::FileType;
use async_trait::async_trait;

struct State {
    objects: Vec<OwnershipEntry>,
    package_store: LocalDBPackageStore,
    resolver: Resolver<PackageCache>,
}

pub struct OwnershipHandler {
    state: Mutex<State>,
    package_filter: Option<ObjectID>,
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
        //Process Sui input_objects to get old ownership information
        let mut old_ownership_entries = Vec::new();
        for object in &checkpoint_transaction.input_objects {
            if object.coin_type_maybe().map(|t| t.to_string()) == Some("0x2::sui::SUI".to_string()) {
                let owner_address = get_owner_address(object);
                let coin_type = object.coin_type_maybe().map(|t| t.to_string()).unwrap_or_else(|| "None".to_string());
                let old_entry = OwnershipEntry {
                    object_id: object.id().to_string(),
                    version: object.version().value().try_into().unwrap(),
                    checkpoint: checkpoint.try_into().unwrap(),
                    epoch: epoch.try_into().unwrap(),
                    timestamp_ms: timestamp_ms.try_into().unwrap(),
                    owner_type: Some(get_owner_type(object)).map(|ot| ot.to_string()),
                    owner_address: owner_address.clone(),
                    object_status: "Transfer Out".to_string(),
                    previous_transaction: object.previous_transaction.base58_encode(),
                    coin_type: Some(coin_type.clone()),
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
                old_ownership_entries.push((object.id(), old_entry.clone()));
            }
        }

        //Process output_objects to get new ownership information and compare with old ownership
        for object in checkpoint_transaction.output_objects.iter() {
            if object.coin_type_maybe().map(|t| t.to_string()) == Some("0x2::sui::SUI".to_string()) {
                // state.package_store.update(object)?;
                let new_owner_address = get_owner_address(object);
                let object_id = object.id();
                if let Some((_, old_entry)) = old_ownership_entries.iter().find(|(id, _)| id == &object_id) {
                    if old_entry.owner_address != new_owner_address {
                        //Ownership has changed. Create a "Transfer Out" entry for the old owner
                        let transfer_out_entry = OwnershipEntry {
                            object_id: object_id.to_string(),
                            version: object.version().value().try_into().unwrap(),
                            checkpoint: checkpoint.try_into().unwrap(),
                            epoch: epoch.try_into().unwrap(),
                            timestamp_ms: timestamp_ms.try_into().unwrap(),
                            owner_type: Some(get_owner_type(object)).map(|ot| ot.to_string()),
                            owner_address: old_entry.owner_address.clone(),
                            object_status: "Transfer Out".to_string(),
                            previous_transaction: object.previous_transaction.base58_encode(),
                            coin_type: object.coin_type_maybe().map(|t| t.to_string()),
                            coin_balance: 0,
                            previous_owner: old_entry.owner_address.clone(),
                            previous_version: Some(old_entry.version),
                            previous_checkpoint: Some(old_entry.checkpoint),
                            previous_coin_type: old_entry.coin_type.clone(),
                            previous_type: old_entry.owner_type.clone(),
                        };
                        state.objects.push(transfer_out_entry);

                        //Entry for the new owner
                        let new_entry = OwnershipEntry {
                            object_id: object_id.to_string(),
                            version: object.version().value().try_into().unwrap(),
                            checkpoint: checkpoint.try_into().unwrap(),
                            epoch: epoch.try_into().unwrap(),
                            timestamp_ms: timestamp_ms.try_into().unwrap(),
                            owner_type: Some(get_owner_type(object)).map(|ot| ot.to_string()),
                            owner_address: new_owner_address.clone(),
                            object_status: "Transfer In".to_string(),
                            previous_transaction: object.previous_transaction.base58_encode(),
                            coin_type: object.coin_type_maybe().map(|t| t.to_string()),
                            coin_balance: if object.coin_type_maybe().is_some() {
                                object.get_coin_value_unsafe().try_into().unwrap()
                            } else {
                                0
                            },
                            previous_owner: old_entry.owner_address.clone(),
                            previous_version: Some(old_entry.version),
                            previous_checkpoint: Some(old_entry.checkpoint),
                            previous_coin_type: old_entry.coin_type.clone(),
                            previous_type: old_entry.owner_type.clone(),
                        };
                        state.objects.push(new_entry);
                    }
                    else {
                        //No ownership change. Mutated object with new balance same owner
                        let object_status_tracker = ObjectStatusTracker::new(effects);
    
                        let new_entry = OwnershipEntry {
                            object_id: object_id.to_string(),
                            version: object.version().value().try_into().unwrap(),
                            checkpoint: checkpoint.try_into().unwrap(),
                            epoch: epoch.try_into().unwrap(),
                            timestamp_ms: timestamp_ms.try_into().unwrap(),
                            owner_type: Some(get_owner_type(object)).map(|ot| ot.to_string()),
                            owner_address: new_owner_address.clone(),
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
                        state.objects.push(new_entry);
                    }
                }
                else {
                    //No previous owner. New object with "Created" Status
                    let object_status_tracker = ObjectStatusTracker::new(effects);

                    let created_entry = OwnershipEntry {
                        object_id: object_id.to_string(),
                        version: object.version().value().try_into().unwrap(),
                        checkpoint: checkpoint.try_into().unwrap(),
                        epoch: epoch.try_into().unwrap(),
                        timestamp_ms: timestamp_ms.try_into().unwrap(),
                        owner_type: Some(get_owner_type(object)).map(|ot| ot.to_string()),
                        owner_address: new_owner_address.clone(),
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
                    state.objects.push(created_entry);
                }
            }
        }

    for (object_ref, _) in effects.all_removed_objects().iter() {
        let object_id = object_ref.0;
        if let Some((_, old_entry)) = old_ownership_entries.iter().find(|(id, _)| id == &object_id) {
            //Create a "DELETED" entry for the deleted SUI objects/owner
            let deleted_entry = OwnershipEntry {
                object_id: object_id.to_string(),
                version: u64::from(object_ref.1),
                checkpoint: checkpoint.try_into().unwrap(),
                epoch: epoch.try_into().unwrap(),
                timestamp_ms: timestamp_ms.try_into().unwrap(),
                owner_type: old_entry.owner_type.clone(),
                owner_address: old_entry.owner_address.clone(),
                object_status: "DELETED".to_string(),
                previous_transaction: checkpoint_transaction.transaction.digest().base58_encode(),
                coin_type: old_entry.coin_type.clone(),
                coin_balance: 0,
                previous_owner: None,
                previous_version: None,
                previous_checkpoint: None,
                previous_coin_type: None,
                previous_type: None,
            };
            state.objects.push(deleted_entry);
        }
    }

        Ok(())
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
            self.process_transaction(
                checkpoint_summary.epoch,
                checkpoint_summary.sequence_number,
                checkpoint_summary.timestamp_ms,
                checkpoint_transaction,
                &checkpoint_transaction.effects,
                &mut state,
            )
            .await?;
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