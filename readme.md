### install

npm install @slofurno/logzq

### usage

```javascript
const logzq = require('@slofurno/logzq')

let now = Date.now()
let start = now - 1000*60*60*24*3
let client = logzq({token: "x-auth-token", debug: true})

let res = await client.query({start, end: now, query: "service:my-service AND env:production AND message:(GET OR PATCH)"})
```

### getting an auth token

- sign into logz
- open the network tab in your browser and select the xhr/fetch filter
- look for the x-auth-token in the request header
