# FTP Reader

FTP Reader provides the ability to read data storage from remote FTP/SFTP file systems.

## Functionality

### Configuration Example

=== "job/ftp2stream.json"

```json
--8<-- "jobs/ftpreader.json"
```

### Parameters

| Configuration     | Required | Data Type   | Default Value | Description                                                            |
| :---------------- | :------: | ----------- | ------------- | ---------------------------------------------------------------------- |
| protocol          | Yes      | string      | None          | Server protocol, currently supports `ftp` and `sftp` transport protocols |
| host              | Yes      | string      | None          | Server address                                                         |
| port              | No       | int         | 22/21         | If transport protocol is `sftp`, default is 22; if standard ftp protocol, default is 21 |
| timeout           | No       | int         | 60000         | Connection timeout for ftp server, in milliseconds (ms)               |
| connectPattern    | No       | string      | PASV          | Connection mode, only supports `PORT`, `PASV` modes. This parameter is only used for ftp protocol |
| username          | Yes      | string      | None          | FTP server access username                                             |
| password          | No       | string      | None          | FTP server access password                                             |
| useKey            | No       | boolean     | false         | Whether to use private key login, only valid for sftp login           |
| keyPath           | No       | string      | `~/.ssh/id_rsa` | Private key address                                                  |
| keyPass           | No       | string      | None          | Private key password, if no private key password is set, no need to configure this |
| path              | Yes      | list        | None          | Remote FTP file system path information, note that multiple paths can be supported, detailed description below |
| column            | Yes      | `list<map>` | None          | List of fields to read, type specifies the type of source data, see below |
| fieldDelimiter    | Yes      | string      | `,`           | Field delimiter for reading                                            |
| compress          | No       | string      | None          | Text compression type, default empty means no compression. Supports `zip`, `gz`, `bzip2` |
| encoding          | No       | string      | `utf-8`       | File encoding configuration for reading                                |
| skipHeader        | No       | boolean     | false         | CSV format files may have header titles that need to be skipped. Default is not to skip |
| nullFormat        | No       | char        | `\N`          | Define which strings can represent null                                |
| maxTraversalLevel | No       | int         | 100           | Maximum number of folder levels allowed for traversal                  |
| csvReaderConfig   | No       | map         | None          | CSV file reading parameter configuration, Map type. Default values used if not configured, see below |