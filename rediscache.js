const { promisify } = require('util');
var hash = require('object-hash');

class RedisCache {
    constructor(Vars) {
        //this.MaxCount = Vars.MaxCount || null;
        this.KeyField = Vars.KeyField;
        this.SequelizeFieldsToRemove = Vars.SequelizeFieldsToRemove != null ? ["createdAt", "updatedAt"].concat(Vars.SequelizeFieldsToRemove) : ["createdAt", "updatedAt"];
        this.RedisClient = Vars.RedisClient;
        this.CleanUpPeriod = Vars.CleanUpPeriod || 15 * 60 * 1000;
        this.getAsync = promisify(this.RedisClient.get).bind(this.RedisClient);
        this.setAsync = promisify(this.RedisClient.set).bind(this.RedisClient);
        this.delAsync = promisify(this.RedisClient.del).bind(this.RedisClient);
        this.keysAsync = promisify(this.RedisClient.keys).bind(this.RedisClient);
        this.Count = null;
        this.initCount();
        setInterval(
            this.CleanUp.bind(this),
            this.CleanUpPeriod
        )
    }

    async initCount() {
        try {
            this.Count = (await this.keysAsync('*')).length;
        } catch (error) {
            try {
                await this.Clear();
                this.Count = 0;
                return;
            } catch (error) {
                throw new Error("Failed to get remote store items count. Clear failed! Please restart the instance!");
            }
        }
    }

    async Add(item) {
        try {
            var result = await this.Update(item);
            this.Count++;
            return result;
        } catch (error) {
            throw error;
        }
    }

    async Update(item) {
        try {
            item.hash = await this.GetItemHash(item);
            item.StoredDate = new Date();
            return await this.setAsync(item[this.KeyField], await JSON.stringify(item));
        } catch (error) {
            throw error;
        }
    }

    async Remove(Key) {
        try {
            var result = await this.delAsync(Key);
            this.Count--;
            return result;
        } catch (error) {
            throw error;
        }
    }

    async Get(Key) {
        try {
            return JSON.parse(await this.getAsync(Key));
        } catch (error) {
            throw error;
        }
    }

    async GetAll() {
        try {
            var keys = await this.GetAllKeys();
            var items = {};
            for (let i = 0; i < keys.length; i++) {
                items[keys[i]] = await this.Get(keys[i]);
            }
            return items;
        } catch (error) {
            throw error;
        }
    }

    async GetAllKeys() {
        try {
            return await this.keysAsync('*');
        } catch (error) {
            throw error;
        }
    }

    async CleanUp() {
        try {
            var keys = await this.GetAllKeys();
            var count = 0;
            await asyncForEach(keys, async (key) => {
                var data = await this.Get(key);
                var CurrentDate = new Date();
                if (CurrentDate - new Date(data.StoredDate) > this.CleanUpPeriod) {
                    await this.Remove(key);
                    this.Count--;
                    count++;
                }
            });
            return count;
        } catch (error) {
            throw error;
        }
    }

    async Sync(StoreItems) {
        try {
            var items = await this.GetAll();
            var count = 0;
            for (var key in items) {
                var item = items[key];
                var itemHash = item.hash;
                var StoreItem = StoreItems.filter(obj => {
                    return obj[this.KeyField] == item[this.KeyField];
                })[0];
                var StoreItemHash = await this.GetItemHash(StoreItem);
                if (itemHash != StoreItemHash) {
                    await this.Update(StoreItem);
                    count++;
                }
            };
            return count;
        } catch (error) {
            throw error;
        }
    }

    async SyncWithSequelizeModel(Model) {
        try {
            var keys = await this.GetAllKeys();
            var StoreItems = await Model.findAll({
                where: {
                    [this.KeyField]: keys
                }
            });
            for (let i = 0; i < StoreItems.length; i++) {
                StoreItems[i] = await this.RemoveFieldsFromSequelizeItem(StoreItems[i].dataValues);
            }
            return await this.Sync(StoreItems);;
        } catch (error) {
            throw error;
        }
    }

    async GetItemHash(item) {
        try {
            return await hash(item);
        } catch (error) {
            throw error;
        }
    }

    async Clear() {
        try {
            var keys = await this.GetAllKeys();
            var count = 0;
            await asyncForEach(keys, async (key) => {
                await this.Remove(key);
                this.Count--;
                count++;
            })
            return count;
        } catch (error) {
            throw error;
        }
    }

    async RemoveFieldsFromSequelizeItem(item) {
        for (const key in this.SequelizeFieldsToRemove) {
            delete item[this.SequelizeFieldsToRemove[key]];
        }
        return item;
    }
}

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array)
    }
}

module.exports = RedisCache;