const fs = require('fs')
const { Client, HttpConnection } = require('@elastic/elasticsearch')

const client = new Client({
    node: 'https://localhost:9200',
    auth: {
        username: 'elastic',
        password: 'asddasdsfdasasdf'
    },
    Connection: HttpConnection,
    tls: {
        ca: fs.readFileSync('./certs/ca.crt'),
        rejectUnauthorized: false
    }
})

const schema = {
    id: { type: 'integer' },
    description: { type: 'text' },
    quantity: { type: 'float' },
    tag: { type: 'object' }
}

const index = 'products2'

async function create() {
    await client.indices.create(
        {
            index,
            operations: {
                mappings: {
                    properties: schema
                }
            }
        },
        { ignore: [400] }
    )

    const sales = fs.readFileSync('./data-product-2.json')

    const parsedTagsSales = JSON.parse(sales).map(sale => {
        return { ...sale, tags: JSON.parse(sale.tag) }
    })


    const operations = parsedTagsSales.flatMap((doc) => [
        { index: { _index: index } },
        doc
    ])

    process.exit(0)

    const bulkResponse = await client.bulk({ refresh: true, operations })

    if (bulkResponse.errors) {
        const erroredDocuments = []
        // The items array has the same order of the dataset we just indexed.
        // The presence of the `error` key indicates that the operation
        // that we did for the document has failed.
        bulkResponse.items.forEach((action, i) => {
            const operation = Object.keys(action)[0]
            if (action[operation].error) {
                erroredDocuments.push({
                    // If the status is 429 it means that you can retry the document,
                    // otherwise it's very likely a mapping error, and you should
                    // fix the document before to try it again.
                    status: action[operation].status,
                    error: action[operation].error,
                    operation: body[i * 2],
                    document: body[i * 2 + 1]
                })
            }
        })
        console.log(erroredDocuments)
    }

    const count = await client.count({ index })
    console.log(count)
}

//   search()

const search = async () => {
    const result = await client.search({
        index,
        pretty: true,
        query: {
            match: {
                quantity: 1
            }
        }
    },
    )

    console.log(JSON.result.hits.hits)
}

// search()
create()

//   run().catch(console.log)
