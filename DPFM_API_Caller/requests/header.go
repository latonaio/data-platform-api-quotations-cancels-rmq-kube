package requests

type Header struct {
	DeliveryDocument   	 int     `json:"DeliveryDocument"`
	HeaderDeliveryStatus *string `json:"HeaderDeliveryStatus"`
	IsCancelled          *bool   `json:"IsCancelled"`
}
