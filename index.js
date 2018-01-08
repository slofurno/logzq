const https = require('https')
const zlib = require('zlib')
const dns = require('dns')

const hour = 1000 * 60 * 60
const defaultTimestep = hour * 12
const hostname = 'app.logz.io'

function makeOptions(token) {
  return new Promise((resolve, reject) => {
    const options = {
      family: 4,
    }

    dns.lookup(hostname, options, (err, address, family) => {
      if (err) { return reject(err) }

      let headers = {
        'accept-encoding' : 'gzip',
        'kbn-version':  '5.5.3',
        'content-type': 'application/x-ndjson',
        'accept': 'application/json, text/plain, */*',
        'x-auth-token': token,
        'host': hostname,
      }

      resolve({
        method: 'POST',
        headers,
        hostname: address,
        path: '/kibana/elasticsearch/_msearch',
      })
    })
  })
}

function dateIndex(date) {
  const prefix = 'logzioCustomerIndex'
  let dd = ("0" + date.getUTCDate()).slice(-2)
  let mm = ("0" + (date.getUTCMonth() + 1)).slice(-2)
  let yy = ("" + date.getUTCFullYear()).slice(-2)
  return prefix + yy + mm + dd
}

function makeBody({query, t0, t1, from, size, index}) {
  let first = {index, ignore_unavailable: true}
  let second = {
    version: true,
    from,
    size,
    sort: [{"@timestamp": {order: "desc", unmapped_type: "boolean"}}],
    query: {bool: {must: [{"query_string": {query,analyze_wildcard:true}}, {range:{"@timestamp": {gte:t0,lte:t1}}}], must_not: []}},
    _source: {excludes: []},
    aggs: {},
    stored_fields: ["*"],
    script_fields: {},
    docvalue_fields: ["@timestamp"],
    highlight: {},
  }

  return JSON.stringify(first) + '\n' + JSON.stringify(second) + '\n'
}

function logzq({token, debug}) {
  async function query({query, start, end, timestep = defaultTimestep}) {

    const options = await makeOptions(token)
    const index = function() {
      let ret = []
      for(let t = start; t <= end; t += hour * 24) {
        ret.push(dateIndex(new Date(t)))
      }
      return ret
    }()

    let ret = []

    for(let t0 = start; t0 < end; t0 += timestep) {
      let t1 = t0 + timestep
      let xs = await doStep({query, t0, t1, index, options})
      ret.push(...xs)
    }
    return ret
  }

  async function doStep({query, t0, t1, index, options}) {
    let size = 500
    let hard_cap = 10000
    let total = size
    let ret = []

    for (let from = 0; from < total; from+=size) {
      let body = makeBody({query, t0, t1, from, size, index})

      let x = await doRequest(options, body)
      let res = x.responses[0]

      if (res.error) {
        throw res.error.type
      }

      total = res.hits.total
      if (total > hard_cap) {
        debug && console.error('[INFO] total (%d) > hard cap; spliting timestep', total)
        let mid = Math.floor(t0 + (t1 - t0) / 2)

        let ret = await doStep({query, t0, t1: mid, index, options})
        let rest = await doStep({query, t0: mid + 1, t1, index, options})
        ret.push(...rest)
        return ret
      }

      let xs = parseMessages(res)
      debug && console.error('[INFO] t0: %d, t1: %d, completed: %d/%d', t0, t1, from + xs.length, total)
      ret.push(...xs)
    }

    return ret
  }

  return {
    query,
  }
}



function doRequest(options, body) {
  return new Promise((resolve, reject) => {
    let req = https.request(options, res => {
      if (res.statusCode !== 200) {
        return reject(res.statusCode)
      }

      let output = res.pipe(zlib.createGunzip())


      let chunks = []
      output.on('data', chunk => {
        chunks.push(chunk.toString())
      })

      output.on('end', () => {
        resolve(JSON.parse(chunks.join('')))
      })
    })

    req.on('error', e => {
      reject(e)
    })

    req.write(body)
    req.end()
  })
}

function parseMessages(res) {
  return res.hits.hits.map(x => x._source)
}


module.exports = logzq
