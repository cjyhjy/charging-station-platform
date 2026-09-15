import { api, unsupported } from './http'
import { flattenPage, legacyToChargerStatus, mapCharger } from './contract'

/** 设备接口（Go 契约）。写入均携带幂等键（16..128，服务端强制）。 */

/**
 * 设备列表；Go 契约参数为 stationId、status、page、pageSize（无 keyword/chargerType）。
 * keyword/chargerType 过滤在 Go 契约补齐前不提交，避免伪装成已生效的筛选。
 */
export function fetchChargers(params = {}) {
  const { stationId, status, page, pageSize } = params
  return api
    .get('/admin/chargers', {
      stationId,
      status: status === undefined || status === null ? undefined : legacyToChargerStatus(status),
      page,
      pageSize
    })
    .then(flattenPage)
    .then(data => ({ ...data, items: (Array.isArray(data.items) ? data.items : []).map(mapCharger) }))
}

/** 远程重启：Go 返回 202 + {commandNo, status}，无命令查询端点（见 fetchDeviceCommand）。 */
export function createRestartCommand(chargerId, { reason }, { idempotencyKey } = {}) {
  return api.post(
    `/admin/chargers/${encodeURIComponent(chargerId)}/restart`,
    { reason },
    { idempotent: true, idempotencyKey }
  )
}

/** 批量创建设备：Go 契约暂无该端点（待 B-01 决策）。 */
export function createChargersBatch() {
  return unsupported('批量创建设备在 Go 后端暂未提供（待 B-01 契约决策）')
}

/** 直接设置设备状态：Go 契约暂无该端点（仅有 release/restart）。 */
export function setChargerStatus() {
  return unsupported('直接设置设备状态在 Go 后端暂未提供（待 B-01 契约决策）')
}

/** 命令状态查询：Go 契约暂无该端点，重启结果经设备列表刷新观察。 */
export function fetchDeviceCommand() {
  return unsupported('命令状态查询在 Go 后端暂未提供；请刷新设备列表查看结果')
}
