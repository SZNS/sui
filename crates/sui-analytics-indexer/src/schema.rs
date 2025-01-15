use diesel::table;

table! {
    ownership (object_id) {
        object_id -> Varchar,
        version -> Int8,
        checkpoint -> Int8,
        epoch -> Int8,
        timestamp_ms -> Int8,
        owner_type -> Nullable<Varchar>,
        owner_address -> Nullable<Varchar>,
        object_status -> Varchar,
        previous_transaction -> Varchar,
        coin_type -> Nullable<Varchar>,
        coin_balance -> Int8,
        previous_owner -> Nullable<Varchar>,
        previous_version -> Nullable<Int8>,
        previous_checkpoint -> Nullable<Int8>,
        previous_coin_type -> Nullable<Varchar>,
        previous_type -> Nullable<Varchar>,
    }
}