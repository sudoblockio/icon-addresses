package routines

import (
	"encoding/json"

	"github.com/geometry-labs/icon-addresses/crud"
	"github.com/geometry-labs/icon-addresses/models"
	"go.uber.org/zap"
)

func StartAddressTypeRoutine() {

	// routine every day
	go addressTypeRoutine()
}

// NOTE this routine only ruins once
func addressTypeRoutine() {

	var addressListRaw interface{}
	json.Unmarshal([]byte(ADDRESS_LIST), &addressListRaw)

	addressList := addressListRaw.([]interface{})

	for _, a := range addressList {
		addressType := a.(map[string]interface{})

		address := addressType["Address"].(string)
		_type := addressType["Type"].(string)

		// Update Postgres
		newAddress := &models.Address{
			PublicKey: address,
			Type:      _type,
		}
		err := crud.GetAddressModel().UpsertOne(newAddress)
		if err != nil {
			zap.S().Warn("Routine=AddressType, Address=", address, " Error=", err.Error())
		}
	}
}

const ADDRESS_LIST = `[
  {
    "Address": "hx0cc3a3d55ed55df7c8eee926a4fafb5412d0cca4",
    "Type": "treasury"
  },
  {
    "Address": "hx10ed7a7065d920e146c86d3915491f5a67248647",
    "Type": "treasury"
  },
  {
    "Address": "hx1216aa95cf2aea0a387b7c243412022f3d7cf29f",
    "Type": "treasury"
  },
  {
    "Address": "hx206846faa5ba46a3717e7349bff9c20d6fe1bba3",
    "Type": "treasury"
  },
  {
    "Address": "hx25c5dace83bceae42c11360a07c9e42a3b5c6122",
    "Type": "treasury"
  },
  {
    "Address": "hx266c053380ad84224ea64ab4fa05541dccc56f5f",
    "Type": "treasury"
  },
  {
    "Address": "hx27664cffd284b8cf488eefb7880f55ce82f42297",
    "Type": "treasury"
  },
  {
    "Address": "hx314348ecbaf01ff6c65c2877a6c593a5facecb35",
    "Type": "treasury"
  },
  {
    "Address": "hx3f945d146a87552487ad70a050eebfa2564e8e5c",
    "Type": "treasury"
  },
  {
    "Address": "hx465a11b018bf72febe40d54bce71f3860695d4c2",
    "Type": "treasury"
  },
  {
    "Address": "hx4d83813703f81cdb85f952a1d1ee736faf732655",
    "Type": "treasury"
  },
  {
    "Address": "hx4df036dcb1809743e677681d43cf2e904421f1eb",
    "Type": "treasury"
  },
  {
    "Address": "hx558b4cd8cd7c25fa25e3109414bb385e3c369660",
    "Type": "treasury"
  },
  {
    "Address": "hx64e40ddd929de5d15b2aab2ef4a3a47f8fe40b3b",
    "Type": "treasury"
  },
  {
    "Address": "hx6b38701ddc411e6f4e84a04f6abade7661a207e2",
    "Type": "treasury"
  },
  {
    "Address": "hx6c22cdba886614d3173e3d2499dc1597bdb57f2c",
    "Type": "treasury"
  },
  {
    "Address": "hx6d2240f9c5fd0db5df6977ee586c3d74e1b1e4aa",
    "Type": "treasury"
  },
  {
    "Address": "hx7062a97bed64624846f3134fdab3fb856dce7075",
    "Type": "treasury"
  },
  {
    "Address": "hx7cdec6f51903ec274e01722bed4c60b6e88ebcbd",
    "Type": "treasury"
  },
  {
    "Address": "hx87b6da94535754c2baee9d69010eb1b91eaa4c37",
    "Type": "treasury"
  },
  {
    "Address": "hx8913f49afe7f01ff0d7318b98f7b4ae9d3cd0d61",
    "Type": "treasury"
  },
  {
    "Address": "hx8d6aa6dce658688c76341b7f70a56dce5361e7ef",
    "Type": "treasury"
  },
  {
    "Address": "hx930bb66751f476babc2d49901cf77429c5cf05c1",
    "Type": "treasury"
  },
  {
    "Address": "hx94a7cd360a40cbf39e92ac91195c2ee3c81940a6",
    "Type": "treasury"
  },
  {
    "Address": "hx980ab0c7473013f656339795a1c63bf44898ce95",
    "Type": "treasury"
  },
  {
    "Address": "hx9913b07fbb31f5e334547bdaa880a767b52e45e1",
    "Type": "treasury"
  },
  {
    "Address": "hx9d9ad1bc19319bd5cdb5516773c0e376db83b644",
    "Type": "treasury"
  },
  {
    "Address": "hx9db3998119addefc2b34eaf408f27ab8103edaef",
    "Type": "treasury"
  },
  {
    "Address": "hx9e19d60c9d6a0ecc2bcace688eff9053622c0c4c",
    "Type": "treasury"
  },
  {
    "Address": "hxa55446e81997c03ee856a58ee18432325a4ef924",
    "Type": "treasury"
  },
  {
    "Address": "hxa9c54005bfa47bb8c3ff0d8adb5ddaac141556a3",
    "Type": "treasury"
  },
  {
    "Address": "hxaafc8af9559d5d320745345ec006b0b2170194aa",
    "Type": "treasury"
  },
  {
    "Address": "hxabdde23cda5b425e71907515940a8f23e29a3134",
    "Type": "treasury"
  },
  {
    "Address": "hxb7750699ca417561b170a980017bfc5fc9cef42e",
    "Type": "treasury"
  },
  {
    "Address": "hxbc2f530a7cb6170daae5876fd24d5d81170b93fe",
    "Type": "treasury"
  },
  {
    "Address": "hxc05ec08b6446a2a16b64eb19b96ea02225b840ab",
    "Type": "treasury"
  },
  {
    "Address": "hxc1481b2459afdbbde302ab528665b8603f7014dc",
    "Type": "treasury"
  },
  {
    "Address": "hxc17ff524858dd51722367c5b04770936a78818de",
    "Type": "treasury"
  },
  {
    "Address": "hxcd6f04b2a5184715ca89e523b6c823ceef2f9c3d",
    "Type": "treasury"
  },
  {
    "Address": "hxcf1b360dbb5818940acc05198d9966e639380b54",
    "Type": "treasury"
  },
  {
    "Address": "hxd3b53e10d8c4c755879be09ff9ba975069664b7a",
    "Type": "treasury"
  },
  {
    "Address": "hxd3f062437b70ab6d6a5f21b208ede64973f70567",
    "Type": "treasury"
  },
  {
    "Address": "hxd42f6e3abfb7f5b14dbdafa34f03ffecf2a53a92",
    "Type": "treasury"
  },
  {
    "Address": "hxd8ba6317da2eec0d9d7d1feed4c9c1f3cf358ae1",
    "Type": "treasury"
  },
  {
    "Address": "hxdd4bc4937923dc140adba57916e3559d039f4203",
    "Type": "treasury"
  },
  {
    "Address": "hxded0165517700240279be84d532b683a8531d76d",
    "Type": "treasury"
  },
  {
    "Address": "hxdf6bd350edae21f84e0a12392c17eac7e04817e7",
    "Type": "treasury"
  },
  {
    "Address": "hxe322ab9b11b63c89b85b9bc7b23350b1d6604595",
    "Type": "treasury"
  },
  {
    "Address": "hxf1b55731e7f597c4e2b8014b5bfb05ce4976d6bc",
    "Type": "treasury"
  },
  {
    "Address": "hxf1e3d780c589901d8af69629d1ffae0ff8c92b1d",
    "Type": "treasury"
  },
  {
    "Address": "hxfc7888bf63d45df125cf567fd8753c05facb3d12",
    "Type": "treasury"
  }
]`
