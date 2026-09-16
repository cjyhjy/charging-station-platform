/**
 * 订单页申诉入口的回归测试。
 *
 * 背景：契约规定"一单一申诉"——同一订单不同内容的重复提交返回 409。
 * 视图此前把 409 直接当成"申诉已提交"，用户会以为这次填的内容也被受理了；
 * 实际上只有第一次的内容存下来了。
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { createMemoryHistory, createRouter } from 'vue-router'
import OrdersView from '../src/views/OrdersView.vue'
import { setAccessToken, clearAccessToken } from '../src/api/http'
import { useAuthStore } from '../src/stores/auth'

const ORDER_NO = 'ORD20260916000000abcd'

/** Go 契约订单（已完成、已支付）：订单页的申诉入口只对 COMPLETED 展示。 */
const COMPLETED_ORDER = {
  orderNo: ORDER_NO,
  userId: 32,
  stationId: 3,
  chargerId: 28,
  status: 'COMPLETED',
  amountCent: 180,
  paidCent: 180,
  paymentStatus: 'PAID',
  energyWh: 1200,
  createdAt: '2026-09-16T00:00:00Z',
  updatedAt: '2026-09-16T00:30:00Z'
}

function envelope(data, { status = 200, code = 0, message = 'ok' } = {}) {
  const success = code === 0 && status < 300
  return {
    ok: success,
    status,
    json: async () => ({ success, code, message, userMessage: '', requestId: 'req-1', data: success ? data : null })
  }
}

/** 服务端替身：订单列表与详情正常，申诉提交按传入状态码作答。 */
function stubServer(appealResponse) {
  return vi.fn(async (url, init) => {
    if (String(url).includes('/appeal')) return appealResponse
    if (/\/api\/v1\/orders\/[^/]+$/.test(String(url))) return envelope(COMPLETED_ORDER)
    if (String(url).startsWith('/api/v1/orders')) {
      return envelope({ items: [COMPLETED_ORDER], meta: { page: 1, pageSize: 20, total: 1 } })
    }
    if (String(url).startsWith('/api/v1/stations')) return envelope({ items: [], meta: { page: 1, pageSize: 50, total: 0 } })
    if (String(url).startsWith('/api/v1/me')) return envelope({ id: 32, displayName: '车主' })
    if (String(url).startsWith('/api/v1/wallet')) return envelope({ balanceCent: 5000, availableCent: 5000 })
    void init
    return envelope({})
  })
}

async function mountView() {
  const renderErrors = []
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/', component: { template: '<div />' } },
      { path: '/orders', component: { template: '<div />' } },
      { path: '/charging', component: { template: '<div />' } },
      { path: '/profile', component: { template: '<div />' } },
      { path: '/:pathMatch(.*)*', component: { template: '<div />' } }
    ]
  })
  await router.push('/orders')
  await router.isReady()
  const wrapper = mount(OrdersView, {
    global: { plugins: [router], config: { errorHandler: error => renderErrors.push(error) } }
  })
  await flushPromises()
  // 展开第一张订单，渲染小票与申诉入口
  await wrapper.get(`[data-testid="order-${ORDER_NO}"]`).trigger('click')
  await flushPromises()
  return { wrapper, renderErrors }
}

beforeEach(() => {
  setActivePinia(createPinia())
  const auth = useAuthStore()
  setAccessToken('user-token-1')
  auth.token = 'user-token-1'
})

afterEach(() => {
  vi.unstubAllGlobals()
  clearAccessToken()
})

describe('订单申诉入口', () => {
  it('提交成功时显示已提交，并提示审核通过后退款', async () => {
    vi.stubGlobal('fetch', stubServer(envelope({ id: 1, orderNo: ORDER_NO, status: 'PENDING', reason: '计量争议' }, { status: 201 })))
    const { wrapper } = await mountView()

    await wrapper.get('[data-testid="appeal-reason"]').setValue('计量争议')
    await wrapper.get('[data-testid="appeal-submit"]').trigger('submit')
    await flushPromises()

    expect(wrapper.find('[data-testid="order-appeal-done"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="order-appeal-exists"]').exists()).toBe(false)
    wrapper.unmount()
  })

  it('409（该订单已有申诉）不能显示成"已提交"', async () => {
    vi.stubGlobal(
      'fetch',
      stubServer(envelope(null, { status: 409, code: 5, message: 'already exists with different content or state' }))
    )
    const { wrapper } = await mountView()

    await wrapper.get('[data-testid="appeal-reason"]').setValue('换一条不同的理由')
    await wrapper.get('[data-testid="appeal-submit"]').trigger('submit')
    await flushPromises()

    expect(wrapper.find('[data-testid="order-appeal-done"]').exists()).toBe(false)
    const exists = wrapper.get('[data-testid="order-appeal-exists"]')
    expect(exists.text()).toContain('已有申诉记录')
    expect(exists.text()).toContain('没有提交')
    wrapper.unmount()
  })
})

describe('订单详情的渲染守卫', () => {
  it('点开订单时 receipt 尚未返回也不能抛渲染错误', async () => {
    vi.stubGlobal('fetch', stubServer(envelope({ id: 1, status: 'PENDING' }, { status: 201 })))
    const { wrapper, renderErrors } = await mountView()

    // 详情请求在途时 receipt 为 null：以前 `receipt.status === 'COMPLETED'` 会抛
    // "Cannot read properties of null (reading 'status')"，评测/申诉两个盒子都渲染不出来。
    expect(renderErrors).toEqual([])
    expect(wrapper.get('[data-testid="order-appeal"]').exists()).toBe(true)
    expect(wrapper.get('[data-testid="order-review"]').exists()).toBe(true)
    wrapper.unmount()
  })
})
