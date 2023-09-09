package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-delivery-document-cancels-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-delivery-document-cancels-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) HeaderRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.DeliveryDocument = %d ", input.DeliveryDocument.DeliveryDocument)
	if input.DeliveryDocument.HeaderDeliveryStatus != nil {
		where = fmt.Sprintf("%s \n AND HeaderDeliveryStatus = %s ", where, *input.DeliveryDocument.HeaderDeliveryStatus)
	}

	where = fmt.Sprintf("%s \n AND ( header.DeliverToParty = %d OR header.DeliverFromParty = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	// where = fmt.Sprintf("%s \n AND ( header.IsCancelled, header.IsMarkedForDeletion ) = ( false, false ) ", where)
	rows, err := c.db.Query(
		`SELECT 
			header.DeliveryDocument
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_delivery_document_header_data as header 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ItemsRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Item {
	where := fmt.Sprintf("WHERE item.DeliveryDocument IS NOT NULL\nAND header.DeliveryDocument = %d", input.DeliveryDocument.DeliveryDocument)
	// where = fmt.Sprintf("%s \n AND ItemDeliveryStatus = ", where, input.DeliveryDocument.HeaderDeliveryStatus)
	where = fmt.Sprintf("%s \n AND ( header.DeliverToParty = %d OR header.DeliverFromParty = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	// where = fmt.Sprintf("%s \n AND ( item.IsCancelled, item.IsMarkedForDeletion ) = ( false, false ) ", where)
	rows, err := c.db.Query(
		`SELECT 
			item.DeliveryDocument, item.DeliveryDocumentItem
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_delivery_document_item_data as item
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_delivery_document_header_data as header
		ON header.DeliveryDocument = item.DeliveryDocument ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToItem(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
