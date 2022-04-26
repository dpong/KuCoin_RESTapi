package kucoinapi

import "net/http"

type PublicTokenResponse struct {
	Code string `json:"code"`
	Data struct {
		Instanceservers []struct {
			Endpoint     string `json:"endpoint"`
			Protocol     string `json:"protocol"`
			Encrypt      bool   `json:"encrypt"`
			Pinginterval int    `json:"pingInterval"`
			Pingtimeout  int    `json:"pingTimeout"`
		} `json:"instanceServers"`
		Token string `json:"token"`
	} `json:"data"`
}

func (b *Client) PublicToken() (*PublicTokenResponse, error) {
	res, err := b.do("spot", http.MethodPost, "api/v1/bullet-public", nil, false, false)
	if err != nil {
		return nil, err
	}
	result := &PublicTokenResponse{}
	err = json.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
