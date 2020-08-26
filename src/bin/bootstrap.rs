use async_std::task;
use lambda_runtime::{error::HandlerError, lambda, Context};
use rusoto_core::Region;
use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, PutItemInput};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;
use std::error::Error;

#[derive(Deserialize)]
pub(crate) struct OrderStartedEvent {
    id: String,
    card_type_id: usize,
    card_number: String,
    card_security_number: String,
}

#[derive(Serialize)]
pub(crate) struct OrderCompleted {
    message: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    lambda!(handle_event);
    Ok(())
}

fn handle_event(e: OrderStartedEvent, _: Context) -> Result<OrderCompleted, HandlerError> {
    task::block_on(insert_to_dynamodb(e));
    Ok(OrderCompleted {
        message: "order finished".to_string(),
    })
}

async fn insert_to_dynamodb(e: OrderStartedEvent) {
    let client = DynamoDbClient::new(Region::ApNortheast1);
    let put_req = PutItemInput {
        item: HashMap::from(e),
        table_name: "order_started_event".to_string(),
        ..Default::default()
    };

    client.put_item(put_req).await.unwrap();
}

impl From<OrderStartedEvent> for HashMap<String, AttributeValue> {
    fn from(e: OrderStartedEvent) -> Self {
        let mut attribute_map = HashMap::new();
        {
            let attr = AttributeValue {
                s: Some(e.id),
                ..Default::default()
            };
            attribute_map.insert("id".to_string(), attr);
        }
        {
            let attr = AttributeValue {
                n: Some(e.card_type_id.to_string()),
                ..Default::default()
            };
            attribute_map.insert("card_type_id".to_string(), attr);
        }
        {
            let attr = AttributeValue {
                s: Some(e.card_number),
                ..Default::default()
            };
            attribute_map.insert("card_number".to_string(), attr);
        }
        {
            let attr = AttributeValue {
                s: Some(e.card_security_number),
                ..Default::default()
            };
            attribute_map.insert("card_security_number".to_string(), attr);
        }

        attribute_map
    }
}
