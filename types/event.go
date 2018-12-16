package types

type Event struct {
	EntityId  string `json:"entity_id"`
	EventType string `json:"event_type"`
	Id        string `json:"id"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}
