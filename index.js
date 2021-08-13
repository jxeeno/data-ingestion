const _ = require('lodash');
const { MongoClient } = require('mongodb');
const crypto = require('crypto');

class DataIngestion {
    existingHashes = new Map();
    existingHashesMutatable = new Map();

    constructor(config){
        this.config = config
        this.client = new MongoClient(_.get(this.config, 'mongo.uri'), { useNewUrlParser: true, useUnifiedTopology: true });
        this.viewName = `vw_${_.get(this.config, 'mongo.collection')}`;
        this.collectionName = _.get(this.config, 'mongo.collection');
    }

    async _init(){
        await this.client.connect();
        this.db = this.client.db(_.get(this.config, 'mongo.db'))
        this.collection = this.db.collection(this.collectionName);
        this.currentDateTime = new Date();

        await this._ensureViewsAndIndexes();
        this.viewCollection = this.db.collection(this.viewName);
        await this._loadExistingHashes();
    }

    async _ensureViewsAndIndexes() {
        await this.collection.createIndex({ startDate: 1, endDate: 1 })
        await this.collection.createIndex({ startDate: 1, endDate: 1, key: 1 })

        const collections = await this.db.listCollections().toArray();
        if(!collections.find(c => c.name === this.viewName)){
            await this.db.createCollection(
                this.viewName,
                {viewOn: this.collectionName, pipeline: [
                    {
                        $match: {
                            // startDate: {$lte: this.currentDateTime},
                            endDate: null
                        }
                    },
                    { $replaceRoot: { newRoot: { $mergeObjects: [ { _id: "$_id", "__hash": "$hash", "__key": "$key" }, "$data" ] } } }
                ]},
            )
        }
    }

    async _loadExistingHashes() {
        const hashes = await this.viewCollection.find({
            ..._.get(this.config, 'query', {})
        }, {projection: {'__hash': 1, '__key': 1}}).toArray();

        if(!hashes){
            // no hashes
            console.log(`No existing active entries found`);
        }else{
            console.log(`Found ${hashes.length} existing active entries`);

            for(const entry of hashes){
                this.existingHashes.set(entry.__hash, entry);
                this.existingHashesMutatable.set(entry.__hash, entry._id);
            }
        }
    }

    async upsertEntries(entries){
        await this._init();
        const ops = entries.map(entry => {
            const hash = this.hashEntry(entry);
            const key = this.keyEntry(entry);
            if(this.existingHashesMutatable.has(hash)){
                // data is unmodified
                const _id = this.existingHashesMutatable.get(hash);
                this.existingHashesMutatable.delete(hash);
                if(this.config.pick || this.config.omit){
                    return {
                        updateOne: {
                            filter: {_id},
                            update: {$set: {data: entry, updated: this.currentDateTime}}
                        }
                    }
                }
                
                return;
            }else{
                return {
                    insertOne: {
                        document: {
                            hash,
                            key,
                            data: entry,
                            startDate: this.currentDateTime,
                            endDate: null
                        }
                    }
                }
            }
        }).filter(noop => !!noop).concat([...this.existingHashesMutatable.values()].map(_id => {
            return {
                updateOne: {
                    filter: {_id},
                    update: {$set: {endDate: this.currentDateTime}}
                }
            }
        }))

        if(ops.length > 0){
            const session = this.client.startSession();
            session.startTransaction();
            try{
                console.log(`There are ${ops.length} ops to send`);
                await this.collection.bulkWrite(ops);
                await session.commitTransaction();
                console.log(`All ops sent ${ops.length}`);
            }catch(e){
                console.log(`🐛 was unexpected: ${e.message}`);
            }
            session.endSession();
        }else{
            console.log(`There are no ops to send`);
        }
    }

    keyEntry(entry){
        const keyFn = _.get(this.config, 'keying');
        if(typeof keyFn === 'function'){
            return keyFn(entry);
        }
        return 'default';
    }

    hashEntry(entry){
        const pick = _.get(this.config, 'hashing.pick');
        const omit = _.get(this.config, 'hashing.omit');
        const hashObject = entry;
        if(pick){
            hashObject = _.pick(hashObject, pick);
        }else if(omit){
            hashObject = _.omit(hashObject, omit);
        }

        const hash = crypto.createHash('sha1').update(JSON.stringify(hashObject), 'utf8').digest('hex');
        return hash;
    }

    async close(){
        await this.client.close();
    }
}

module.exports = DataIngestion;