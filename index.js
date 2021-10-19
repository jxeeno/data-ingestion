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
        this.iviewName = `ivw_${_.get(this.config, 'mongo.collection')}`;
        this.collectionName = _.get(this.config, 'mongo.collection');
    }

    async _init(){
        await this.client.connect();
        this.db = this.client.db(_.get(this.config, 'mongo.db'))
        this.collection = this.db.collection(this.collectionName);
        this.currentDateTime = new Date();

        await this._ensureViewsAndIndexes();
        this.viewCollection = this.db.collection(this.viewName);
        this.iviewCollection = this.db.collection(this.iviewName);
        await this._loadExistingHashes();
    }

    async _ensureViewsAndIndexes() {
        await this.collection.createIndex({ startDate: 1, endDate: 1 })
        await this.collection.createIndex({ startDate: 1, endDate: 1, key: 1 })
        await this.collection.createIndex({ endDate: 1, 'data.id': 1 })
        await this.collection.createIndex({ endDate: 1, key: 1 })

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
        
        if(!collections.find(c => c.name === this.iviewName)){
            await this.db.createCollection(
                this.iviewName,
                {viewOn: this.collectionName, pipeline: [
                    {
                        $match: 
                            endDate: null
                        }
                    }
                ]},
            )
        }
    }
    
    static dotNotationDataPrefix (obj) {
        const newObject = {};
        for(const key in obj){
            if(key === '__key'){
                newObject.key = obj[key];
            }else if(key === '__hash'){
                newObject.hash = obj[key];
            }else if(key === '_id'){
                newObject._id = obj[key];
            }else{
                newObject[`data.${key}`] = obj[key];
            }
        }
        return newObject;
    }
    
    async _loadExistingHashes() {
        const hashes = await this.iviewCollection.find({
            ...DataIngestion.dotNotationDataPrefix(_.get(this.config, 'query', {}))
        }, {projection: {'hash': 1, 'key': 1}}).toArray();

        if(!hashes){
            // no hashes
            console.log(`No existing active entries found`);
        }else{
            console.log(`Found ${hashes.length} existing active entries`);

            for(const entry of hashes){
                this.existingHashes.set(entry.hash, entry);

                if(this.existingHashesMutatable.has(entry.hash)){
                    this.existingHashesMutatable.get(entry.hash).push(entry._id);
                }else{
                    this.existingHashesMutatable.set(entry.hash, [entry._id]);
                }
                
            }
        }
    }
    
    async upsertEntries(entries, dryRun = false){
        await this._init();
        const stats = {insert: 0, delete: 0, update: 0}
        const ops = entries.flatMap(entry => {
            const hash = this.hashEntry(entry);
            const key = this.keyEntry(entry);
            if(this.existingHashesMutatable.has(hash)){
                // data is unmodified
                const _ids = this.existingHashesMutatable.get(hash);
                this.existingHashesMutatable.delete(hash);
                if(_.get(this.config, 'hashing.pick') || _.get(this.config, 'hashing.omit')){
                    stats.update++;
                    stats.delete += _ids.length-1;

                    return _ids.map((_id, i) => (i === 0 ? {
                        updateOne: {
                            filter: {_id},
                            update: {$set: {data: entry, updated: this.currentDateTime}}
                        }
                    } : {
                        updateOne: {
                            filter: {_id},
                            update: {$set: {endDate: this.currentDateTime}}
                        }
                    }))
                }
                
                return;
            }else{
                stats.insert++;
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
        }).filter(noop => !!noop).concat([...this.existingHashesMutatable.values()].flatMap(_ids => {
            stats.delete++;
            return _ids.map(_id => ({
                updateOne: {
                    filter: {_id},
                    update: {$set: {endDate: this.currentDateTime}}
                }
            }))
        }))
        
        if(dryRun){
            console.log(`There are ${ops.length} ops to send - ${JSON.stringify(stats)}`);
            return;
        }
        
        if(ops.length > 0){
            const session = this.client.startSession();
            session.startTransaction();
            try{
                console.log(`There are ${ops.length} ops to send - ${JSON.stringify(stats)}`);
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
        let hashObject = entry;
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
