# ds-trocco-mcp

MCP server for using TROCCO API

ref: [gtnao/troccomcp](https://github.com/gtnao/troccomcp)

## build

```shell
npm install
npm run build
```


## VSCode configuration

```json
{
    "inputs": [
        {
            "type": "promptString",
            "id": "trocco_api_key",
            "description": "TROCCO API Key",
            "password": true
        }
    ],
    "servers": {
        "trocco": {
            "command": "node",
            "args": [
                "Repository/SoySoySoyB/trocco-mcp/build/index.js"
            ],
            "env": {
                "TROCCO_API_KEY": "${input:trocco_api_key}"
            }
        }
    }
}
```