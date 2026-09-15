import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useChargersStore } from '../src/stores/chargers'
import { failResponse, installFetch, okResponse } from './helpers'

/** Go Charger 契约字段（type 为 AC/DC 字符串，status 为字符串枚举）。 */
const GO_CHARGER = {
  id: 11,
  stationId: 1,
  code: 'ZGC-DC-01',
  type: 'DC',
  powerWatt: 120000,
  status: 'IDLE'
}

/** 适配层归一化后的行形状（旧数字状态码 + statusText + version 恒 0）。 */
const MAPPED_ROW = {
  id: 11,
  stationId: 1,
  code: 'ZGC-DC-01',
  chargerType: 1,
  powerWatt: 120000,
  connectorStandard: '',
  status: 0,
  statusText: '空闲',
  version: 0
}

function goPage(items, total = items.length) {
  return { items, meta: { page: 1, pageSize: 20, total } }
}

let chargers = null
let harness = null

beforeEach(() => {
  setActivePinia(createPinia())
  sessionStorage.clear()
  localStorage.clear()
  chargers = useChargersStore()
})

afterEach(() => {
  chargers.stopPolling()
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('设备列表查询', () => {
  it('按站点/状态组装查询参数；状态数字码映射为 Go 字符串枚举', async () => {
    harness = installFetch([okResponse(goPage([GO_CHARGER]))])
    await chargers.setFilter({ stationId: 1, status: 0 })

    const query = harness.queryOf(0)
    expect(harness.urlOf(0).startsWith('/api/v1/admin/chargers')).toBe(true)
    expect(query.get('stationId')).toBe('1')
    expect(query.get('status')).toBe('IDLE')
    // Go 契约不支持 keyword/chargerType 过滤：不提交，避免伪装成已生效的筛选。
    expect(query.has('keyword')).toBe(false)
    expect(query.has('chargerType')).toBe(false)
    // 响应归一化：AC/DC → 0/1，字符串状态 → 数字码 + statusText，version 恒 0。
    expect(chargers.items[0].status).toBe(0)
    expect(chargers.items[0].statusText).toBe('空闲')
    expect(chargers.items[0].chargerType).toBe(1)
    expect(chargers.items[0].version).toBe(0)
  })

  it('状态 0（空闲）不会被当成“未提供”而丢失', async () => {
    harness = installFetch([okResponse(goPage([]))])
    chargers.filters = { stationId: null, status: 0 }
    await chargers.load()
    expect(harness.queryOf(0).get('status')).toBe('IDLE')
  })

  it('失败时清空列表并给出可读错误', async () => {
    harness = installFetch([failResponse({ status: 503, code: 3, userMessage: '数据库暂不可用' })])
    await chargers.load()
    expect(chargers.error).toBe('数据库暂不可用')
    expect(chargers.items).toEqual([])
    expect(chargers.isEmpty).toBe(false)
  })
})

describe('设备状态变更与批量创建（Go 契约暂缺）', () => {
  it('直接设置设备状态显式失败，不发起任何请求', async () => {
    harness = installFetch([])
    chargers.items = [{ ...MAPPED_ROW }]
    await expect(chargers.setStatus(chargers.items[0], 2, '人工巡检发现故障')).resolves.toBe(false)
    expect(chargers.error).toContain('暂未提供')
    expect(harness.count()).toBe(0)
  })

  it('批量创建设备显式失败，不发起任何请求', async () => {
    harness = installFetch([])
    const ok = await chargers.batchCreate({ stationId: 1, chargers: [{ code: 'X-01', chargerType: 0, powerWatt: 7000 }] })
    expect(ok).toBe(false)
    expect(chargers.error).toContain('暂未提供')
    expect(harness.count()).toBe(0)
  })
})

describe('远程重启（Go 契约）', () => {
  it('创建重启命令后展示受理结果；无命令查询端点，不自动轮询', async () => {
    harness = installFetch([okResponse({ commandNo: 'CMD202609020001', status: 'PENDING' })])

    chargers.items = [{ ...MAPPED_ROW }]
    await expect(chargers.restart(chargers.items[0], '远程恢复测试')).resolves.toBe(true)

    expect(harness.indexOf('POST', '/admin/chargers/11/restart')).toBe(0)
    // Go 契约不需要 confirm 字段（二次确认由前端承担），仅提交原因。
    expect(harness.bodyOf(0)).toEqual({ reason: '远程恢复测试' })
    expect(harness.headersOf(0)['Idempotency-Key']).toMatch(/^[0-9a-f-]{36}$/)
    expect(chargers.command.commandNo).toBe('CMD202609020001')
    expect(chargers.command.status).toBe('PENDING')
    // Go 契约没有命令状态查询：不进入轮询，提示以列表刷新为准。
    expect(chargers.commandPolling).toBe(false)
    expect(chargers.notice).toContain('稍后刷新列表')
  })

  it('命令状态查询不可用：显式失败而不是打在真实 404 上', async () => {
    chargers.command = { commandNo: 'CMD-1', status: 'PENDING' }
    await expect(chargers.pollCommand()).rejects.toThrow('暂未提供')
  })

  it('重启失败时保留可读错误', async () => {
    harness = installFetch([failResponse({ status: 409, code: 15, userMessage: '当前状态不允许重启' })])
    chargers.items = [{ ...MAPPED_ROW }]
    await expect(chargers.restart(chargers.items[0], '远程恢复测试')).resolves.toBe(false)
    expect(chargers.error).toBe('当前状态不允许重启')
  })
})
