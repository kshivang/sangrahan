import json
from google.auth import jwt
from google.cloud import pubsub_v1

# --- Base variables and auth path
CREDENTIALS_PATH = "serviceAccountKey.json"
PROJECT_ID = "blissful-ly"
TOPIC_ID = "healthkit"
MAX_MESSAGES = 10

# --- PubSub Utils Classes
class PubSubPublisher:
    def __init__(self, credentials_path, project_id, topic_id):
        credentials = jwt.Credentials.from_service_account_info(
            json.load(open(credentials_path)),
            audience="https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        )
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def publish(self, data: str):
        result = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        return result


# --- Main publishing script
def main():
    publisher = PubSubPublisher(CREDENTIALS_PATH, PROJECT_ID, TOPIC_ID)
    data = {
    "uid": "Himanshu Pandey",
    "start_time": "2022-12-08 07:47:10.472892 UTC",
    "end_time": "2022-12-08 07:47:10.472892 UTC",
    "health_data": [{
        "key": "BP",
        "value": {
            "time_series": [{
                    "timestamp": "2022-12-08 07:47:10.472892 UTC",
                    "value": {
                        "int_value": 120
                    }
                    },
                    {
                    "timestamp": "2022-12-08 07:47:11.472892 UTC",
                    "value": {
                        "int_value": 121
                    }
                    },
                    {
                    "timestamp": "2022-12-08 07:47:12.472892 UTC",
                    "value": {
                        "int_value": 122
                    }
                    },
            ]
        },
    }]
    }
        
    future = publisher.publish(json.dumps(data))
    print(future.result())
    
if __name__ == "__main__":
    main()