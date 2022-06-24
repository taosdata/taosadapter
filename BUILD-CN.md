# 构建 taosAdapter

我们强烈建议将 taosAdapter 和 TDengine 服务端共同部署在同一个系统上，并使用官方 TDengine 安装包安装 taosAdapter。如果您想对 taosAdapter 进行调试或贡献代码，您也可以单独构建它。

## 设置 Golang 开发环境

taosAdapter 是由 Go 语言开发的。关于 Golang 开发环境的设置，请参考 Golang 的[官方文档](https://go.dev/learn/)。

请使用 1.14 以上版本的 Golang。对于中国的用户，我们建议使用代理服务来加速软件包的下载。

```shell
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

## 作为 TDengine 的一个组件构建 taosAdapter

taosAdapter 的源代码是作为一个独立的代码库托管的，也通过子模块的方式存在于 TDengine 中。您可以下载 TDengine 的源代码并同时构建它们。步骤如下：

```shell
git clone https://github.com/taosdata/TDengine
cd TDengine
git submodule update --init --recursive
mkdir debug
cd debug
cmake .. -DBUILD_HTTP=false
make
sudo make install
```

一旦 `make install` 完成，taosAdapter 和它的 systemd 服务文件就会被安装到有 TDengine 服务端软件的系统中。您可以使用 `sudo systemctl start taosd` 和 `sudo systemctl stop taosd` 来启动它们。

## 单独构建 taosAdapter

如果您已经部署了 TDengine 服务器 v2.4.0.0 或以上的版本，taosAdapter 也可以作为一个独立的应用程序被构建。

### 安装 TDengine 服务器或客户端安装包

请从官方网站下载 TDengine 服务器或客户端安装包。

### 从源码构建 taosAdapter

``` shell
git clone https://github.com/taosdata/taosadapter
cd taosadapter
go build
```

然后您应该在工作目录中找到 taosAdapter 的二进制可执行文件。您需要将 systemd 服务配置文件 `taosadapter.service` 复制到 `/etc/systemd/system` 目录，并将可执行的 taosAdapter 二进制文件复制到 Linux 的 `$PATH` 环境变量可以找到的路径下。
