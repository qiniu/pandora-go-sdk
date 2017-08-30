package report

import . "github.com/qiniu/pandora-go-sdk/base"

func (c *Report) ActivateUser(input *UserActivateInput) (output *UserActivateOutput, err error) {
	op := c.newOperation(OpActivateUser)

	output = &UserActivateOutput{}
	req := c.newRequest(op, input.Token, output)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)

	return output, req.Send()
}

func (c *Report) CreateDatabase(input *CreateDatabaseInput) (err error) {
	op := c.newOperation(OpCreateDatabase, input.DatabaseName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Report) ListDatabases(input *ListDatabasesInput) (output *ListDatabasesOutput, err error) {
	op := c.newOperation(OpListDatabases)

	output = &ListDatabasesOutput{}
	req := c.newRequest(op, input.Token, output)
	return output, req.Send()
}

func (c *Report) DeleteDatabase(input *DeleteDatabaseInput) (err error) {
	op := c.newOperation(OpDeleteDatabase, input.DatabaseName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Report) CreateTable(input *CreateTableInput) (err error) {
	op := c.newOperation(OpCreateTable, input.DatabaseName, input.TableName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Report) ListTables(input *ListTablesInput) (output *ListTablesOutput, err error) {
	op := c.newOperation(OpListTables, input.DatabaseName)

	output = &ListTablesOutput{}
	req := c.newRequest(op, input.Token, &output)
	return output, req.Send()

}

func (c *Report) UpdateTable(input *UpdateTableInput) (err error) {
	op := c.newOperation(OpUpdateTable, input.DatabaseName, input.TableName)

	req := c.newRequest(op, input.Token, nil)
	if err = req.SetVariantBody(input); err != nil {
		return
	}
	req.SetHeader(HTTPHeaderContentType, ContentTypeJson)
	return req.Send()
}

func (c *Report) DeleteTable(input *DeleteTableInput) (err error) {
	op := c.newOperation(OpDeleteTable, input.DatabaseName, input.TableName)

	req := c.newRequest(op, input.Token, nil)
	return req.Send()
}

func (c *Report) MakeToken(desc *TokenDesc) (string, error) {
	return MakeTokenInternal(c.Config.Ak, c.Config.Sk, desc)
}
