# 历史技术栈归档

本目录归档集成提交中的 C++/Crow/Qt/SQLite、旧客户端、独立设备模拟器、旧 CMake 和运维脚本。
仅用于需求映射和历史实现参考，不作为当前构建、CI 或部署入口；不修复迁移后失效的相对路径。
其中 scripts 是旧脚本，不得用于当前 PostgreSQL 实例。

完整旧目录布局可在独立 worktree 检出归档前的集成提交（见退役说明中的基线），进行历史回归。
`nginx/ncs-api.conf.template` 与 `backend/deploy/` 仍是当前 Go 栈的部署资产，因此留在仓库根目录，未归档到这里。
源码归档不等于业务需求取消。大屏/ML、管理备份界面、二次验证和价格版本等缺口仍须补齐并验收。
参见 [退役与验证说明](../docs/integration/go-vue-retirement.md)。
