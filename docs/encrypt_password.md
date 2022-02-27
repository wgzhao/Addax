# 加密配置文件的中密码

默认情况下，配置文件的密码是明文的，这带来了一定的安全隐患，从 `4.0.9` 版本起，我们增加了对配置文件的的密码加密功能。 同时我们提供了一个 `shell` 脚本 `encrypt_password.sh` 来帮助你加密配置文件的密码。

假定你的原始密码是 `123456`，你希望在配置文件中使用加密密码配置。 首先执行下面的指令：

```shell
$ bin/encrypt_password.sh 123456
The encrypt string is : '${enc:tFd05jnm1mSq+PEK9t/Rgg==}', you can paste it to json file.
```

上述结果中的 `tFd05jnm1mSq+PEK9t/Rgg==` 为 `123456` 的密文。 `${enc:` 开头是为了让 `addax` 知道这是一个加密密文。

你将上述字符串 `${enc:tFd05jnm1mSq+PEK9t/Rgg==}` 替换你的配置文件中设置密码为 `123456` 的地方即可。