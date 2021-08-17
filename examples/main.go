package main

import (
	"context"
	"fmt"

	"github.com/qiniu/pandora-go-sdk/v2/auth"
	"github.com/qiniu/pandora-go-sdk/v2/client"
)

func main() {
	fmt.Println("hello world")
	a, err := test()
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(a)
	}
}

func test() (body map[string]interface{}, err error) {
	cli := &client.DefaultClient
	credentials := auth.New("eyJhbGciOiJIUzUxMiIsInppcCI6IkRFRiJ9.eJwVy0EOwiAQQNG7zBoMDAMWVr3KDJYEU2ltizEx3t26_Xn_A_ejQoIByWcng_ZeWJMga8kYNUZjQ0F0FAQUvOp2dJ4hFZ73ScHe5ZxXbrdl4_FZW-2XvDxO2aRAsgGjtWRMUDC9139w9hqRTPz-AF5EIpU.IEiCAcJpkp0i0WiqqioRfaNXxtBCnyrHaJN3cmiyAZasM98NDAqJm1zFiUomnoITVekg2lOjJqh5cuaRbnqpRA")

	err = cli.CredentialedCall(context.Background(), credentials, auth.PandoraToken, &body, "GET", "http://pandora-express-rc.qiniu.io/api/v1/repos", nil)
	return
}
