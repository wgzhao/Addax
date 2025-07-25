# FTP Writer

FTP Writer provides the ability to write files to remote FTP/SFTP servers, currently only supports writing text files.

## Configuration Example

=== "job/stream2ftp.json"

  ```json
  --8<-- "jobs/ftpwriter.json"
  ```

## Parameters

| Configuration     | Required | Data Type | Default Value   | Description                                                      |
| :---------------- | :------: | --------- | --------------- | ---------------------------------------------------------------- |
| protocol          | Yes      | string    | `ftp`           | Server protocol, currently supports ftp and sftp transport protocols |
| host              | Yes      | string    | None            | Server address                                                   |
| port              | No       | int       | 22/21           | FTP default is 21, SFTP default is 22                          |
| timeout           | No       | int       | `60000`         | Connection timeout for FTP server, in milliseconds (ms)        |
| connectPattern    | No       | string    | `PASV`          | Connection mode, only supports `PORT`, `PASV` modes. Used for FTP protocol |
| username          | Yes      | string    | None            | Username                                                         |
| password          | Yes      | string    | None            | Access password                                                  |
| useKey            | No       | boolean   | false           | Whether to use private key login, only valid for SFTP login    |
| keyPath           | No       | string    | `~/.ssh/id_rsa` | Private key address                                             |
| keyPass           | No       | string    | None            | Private key password, no need to configure if no private key password is set |
| path              | Yes      | string    | None            | Remote FTP file system path information, FtpWriter will write multiple files under Path directory |
| fileName          | Yes      | string    | None            | Name of file to write, this filename will have random suffix added as actual filename for each thread |
| writeMode         | Yes      | string    | None            | Data cleanup processing mode before writing, see below         |
| fieldDelimiter    | Yes      | string    | `,`             | Field delimiter for reading                                      |