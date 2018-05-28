# REST API

## Structure

### GET REQUESTS:
#### Columns:
/api/v1/column/\<id\>/ : Get the info of a single column
```json
{
  "id": 0,
  "name": ""
}
```

#### Databases:
/api/v1/database/ : Get a list of databases
```json
{
  "data": 
  [ 
    {
      "id": 0,
      "name": "",
      "description": ""
    },
    "..."
  ],
  "size": 0
}
```
/api/v1/database/\<id\>/ : Get the info of a single database
```json
{
  "id": 0,
  "name": "",
  "description": "",

  "versions": 
  { 
    "data": 
    [
      {
      
      },
      "..."
    ], 
    "size": 0 
  },
  "users": 
  { 
    "data": 
    [
      {
      
      },
      "..."
    ], 
    "size": 0 
  }
}

```
/api/v1/database/\<id\>/user/ : Get a list of users with access
```json
{ 
  "data": 
  [
    {
      
    },
    "..."
  ], 
  "size": 0 
}
```

#### Roles:
/api/v1/role/
```json
{
  "data":
  [
    {
      "id": 0,
      "name": "",
      "description": ""
    },
    "..."
  ],
  "size": 0
}
```

/api/v1/role/\<id\>/
```json
{
  "id": 0,
  "name": "",
  "description": "",

  "users": 
  {
    "data": 
    [
      {
      
      },
      "..."
    ],
    "size": 0
  }
}
```

/api/v1/role/\<id\>/user/
```json
{
  "data":
  [
    {
    
    },
    "..."
  ],
  "size": 0
}
```

#### Tables:
/api/v1/table/\<id\>/
```json
{
  "id": 0,
  "name": "",

  "columns": 
  {
    "data":
    [
      {
        "id": 0,
        "name": ""
      },
      "..."
    ],
    "size": 0
  }
}
```
/api/v1/table/\<id\>/columns/
```json
{
  "data":
  [
    {
      "id": 0,
      "name": ""
    },
    "..."
  ],
  "size": 0
}
```
/api/v1/table/\<id\>/content?page=&page_size=
```json

```

#### Users:
/api/v1/user/
```json
{
  "data":
  [
    {
      "id": 0,
      "username": "",
      "active": false,
      "firstname": "",
      "lastname": "",
      "email": "example@localhost"
    },
    "..."
  ],
  "size": 0
}
```
/api/v1/user/\<id\>/
```json
{
  "id": 0,
  "username": "",
  "active": false,
  "firstname": "",
  "lastname": "",
  "email": "example@localhost",

  "roles":
  {
    "data":
    [
      {
        "id": 0,
        "name": "",
        "description": ""
      },
      "..."
    ],
    "size": 0
  },
  "databases": 
  {
    "data": 
    [ 
      {
        "id": 0,
        "name": "",
        "description": ""
      },
      "..."    
    ],
    "size": 0
  }
}
```
/api/v1/user/\<id\>/database/
```json
{
  "data": 
  [ 
    {
      "id": 0,
      "name": "",
      "description": ""
    },
    "..."    
  ],
  "size": 0
}
```
/api/v1/user/\<id\>/role/
```json
{
  "data":
  [
    {
      "id": 0,
      "name": "",
      "description": ""
    },
    "..."
  ],
  "size": 0
}
```

#### Versions:
/api/v1/version/\<id\>/
```json
{
  "id": 0,
  "version_number": 0,

  "tables": 
  {
    "data":
    [
      {
        "id": 0,
        "name": ""
      },
      "..."
    ],
    "size": 0
  }
}
```
/api/v1/version/\<id\>/table/
```json
{
  "data":
  [
    {
      "id": 0,
      "name": ""
    },
    "..."
  ],
  "size": 0
}
```

### POST REQUESTS
#### Operations:

