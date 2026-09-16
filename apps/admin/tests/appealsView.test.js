/**
 * 申诉队列的渲染回归测试。
 *
 * 背景：金额列曾经表头写「应付（分）/实付（分）」，但实付单元格渲染的是「1.70 元」，
 * 同一行两种单位；审核通过后要把实付金额退回钱包，管理员读错单位就会误判。
 * 这里锁定"表头与单元格同一口径（元）"。
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import AppealsView from '../src/views/AppealsView.vue'
import { vReveal } from '../src/directives/reveal'
import { flush, installFetch, okResponse, settle } from './helpers'

/** Go Appeal 契约：金额是整数分。 */
const APPEALS = {
  items: [
    {
      id: 7,
      orderNo: 'ORD20260916000000abcd',
      reason: '计量争议：电量与实际不符',
      status: 'PENDING',
      orderAmountCent: 170,
      orderPaidCent: 170,
      createdAt: '2026-09-16T00:30:00Z'
    },
    {
      id: 6,
      orderNo: 'ORD20260916000000eeee',
      reason: '重复扣费',
      status: 'APPROVED',
      orderAmountCent: 3200,
      orderPaidCent: 3200,
      createdAt: '2026-09-15T00:30:00Z',
      decidedAt: '2026-09-15T01:00:00Z'
    }
  ],
  meta: { page: 1, pageSize: 20, total: 2 }
}

function stubMatchMedia() {
  window.matchMedia = vi.fn(() => ({
    matches: false,
    media: '(prefers-reduced-motion: reduce)',
    addEventListener: () => {},
    removeEventListener: () => {},
    addListener: () => {},
    removeListener: () => {}
  }))
}

async function mountView() {
  stubMatchMedia()
  installFetch([okResponse(APPEALS)])
  setActivePinia(createPinia())
  const wrapper = mount(AppealsView, { global: { directives: { reveal: vReveal } } })
  await flush(10)
  await settle(3)
  return wrapper
}

beforeEach(() => {
  sessionStorage.clear()
})

afterEach(() => {
  delete window.matchMedia
  vi.unstubAllGlobals()
})

describe('申诉队列', () => {
  it('金额列表头与单元格同一口径（元），不出现"（分）"配"元"', async () => {
    const wrapper = await mountView()

    const headers = wrapper.findAll('[data-testid="appeals-table"] thead th').map(th => th.text())
    expect(headers).toEqual(['申诉编号', '订单号', '申诉原因', '应付（元）', '实付（元）', '提交时间', '状态'])

    // 行用 DataTable 的 rowTestId 定位：列表体是 TransitionGroup（测试环境里是占位组件），
    // 不能用 tbody tr 选择。
    const firstRow = wrapper.get('[data-testid="appeal-row-7"]')
    expect(firstRow.text()).toContain('1.70')
    expect(firstRow.text()).toContain('ORD20260916000000abcd')
    expect(firstRow.text()).not.toContain('170 分')

    wrapper.unmount()
  })

  it('只有待处理的申诉给出审核按钮', async () => {
    const wrapper = await mountView()

    expect(wrapper.find('[data-testid="appeal-approve-7"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="appeal-approve-6"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="appeal-state-7"]').text()).toBe('待处理')
    expect(wrapper.get('[data-testid="appeal-state-6"]').text()).toBe('已通过')

    wrapper.unmount()
  })
})
