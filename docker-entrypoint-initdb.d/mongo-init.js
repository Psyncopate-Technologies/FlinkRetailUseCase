print('Start #################################################################');

db = db.getSiblingDB('api_prod_db');
db.createUser(
  {
    user: 'api_user',
    pwd: 'api1234',
    roles: [{ role: 'readWrite', db: 'api_prod_db' }],
  },
);
db.createCollection('users');
db.createRole(
  {
      role: "flinkrole",
      privileges: [{
          // Grant privileges on all non-system collections in all databases
          resource: { db: "", collection: "" },
          actions: [
              "splitVector",
              "listDatabases",
              "listCollections",
              "collStats",
              "find",
              "changeStream" ]
      }],
      roles: [
          // Read config.collections and config.chunks
          // for sharded cluster snapshot splitting.
          { role: 'read', db: 'config' }
      ]
  }
);

db.createUser(
{
    user: 'flinkuser',
    pwd: 'flinkpw',
    roles: [
       { role: 'flinkrole', db: 'api_prod_db' }
    ]
}
);

db = db.getSiblingDB('api_dev_db');
db.createUser(
  {
    user: 'api_user',
    pwd: 'api1234',
    roles: [{ role: 'readWrite', db: 'api_dev_db' }],
  },
);
db.createCollection('users');

db = db.getSiblingDB('api_test_db');
db.createUser(
  {
    user: 'api_user',
    pwd: 'api1234',
    roles: [{ role: 'readWrite', db: 'api_test_db' }],
  },
);
db.createCollection('users');

print('END #################################################################');