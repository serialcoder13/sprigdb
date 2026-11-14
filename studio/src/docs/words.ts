// Completion words for the MongoDB editor: shell methods, query and update operators, stages.
export const MONGO_WORDS = [
  'find', 'findOne', 'aggregate', 'countDocuments', 'estimatedDocumentCount', 'distinct', 'insertOne', 'insertMany',
  'updateOne', 'updateMany', 'replaceOne', 'deleteOne', 'deleteMany', 'createIndex', 'getIndexes', 'dropIndex', 'drop',
  'sort', 'limit', 'skip', 'projection', 'hint', 'explain', 'getCollection', 'getSiblingDB', 'runCommand',
  'createCollection', 'getCollectionNames', 'ObjectId', 'ISODate', 'NumberLong', 'NumberInt', 'NumberDecimal',
  'Timestamp', 'UUID', 'BinData', 'MinKey', 'MaxKey',
  '$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin', '$and', '$or', '$nor', '$not', '$exists', '$type',
  '$regex', '$elemMatch', '$size', '$all', '$expr', '$text', '$search',
  '$set', '$unset', '$inc', '$mul', '$rename', '$min', '$max', '$push', '$pull', '$addToSet', '$pop', '$currentDate',
  '$match', '$group', '$project', '$sort', '$limit', '$skip', '$unwind', '$lookup', '$addFields', '$count',
  '$facet', '$bucket', '$replaceRoot', '$sum', '$avg', '$first', '$last', '$push', '$out', '$merge',
];
