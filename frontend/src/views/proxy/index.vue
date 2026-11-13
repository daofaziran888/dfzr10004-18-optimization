<template>
  <section class="page" data-module="proxy">
    <header class="page-head">
      <div>
        <h2>代理池管理</h2>
        <p class="page-desc">维护代理池的节点配置、响应指标与切换策略；调度裁决把响应延迟、可用率、最近失败时间与重试规则冷却放在同一次决策里。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记代理节点</button>
        <button class="btn" type="button" @click="exportRows">导出代理池管理清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="reportNode(row, 'success')">上报成功</button>
            <button class="link" type="button" @click="reportNode(row, 'failure')">上报失败</button>
            <button class="link" type="button" @click="healthCheck(row)">健康校验</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无代理池管理数据，可先登记代理节点</td>
        </tr>
      </tbody>
    </table>

    <section class="panel">
      <h3>代理调度裁决</h3>
      <p class="panel-desc">
        按冷却结束时间、站点限制与优先级给出可执行节点顺序；只有站点归属负责人可以执行受控操作，越权与共享查看只读。
      </p>
      <div class="filter-bar">
        <label class="filter-item">
          <span>采集计划</span>
          <select v-model.number="scheduleForm.planId">
            <option v-for="plan in plans" :key="String(plan.id)" :value="Number(plan.id)">
              {{ plan.计划编号 }} · {{ plan.目标站点 }} · 优先级{{ plan.优先级 }}
            </option>
          </select>
        </label>
        <label class="filter-item">
          <span>操作人</span>
          <input v-model="scheduleForm.operator" placeholder="站点归属负责人，如 张倩" />
        </label>
        <label class="filter-item">
          <span>角色</span>
          <select v-model="scheduleForm.role">
            <option>负责人</option>
            <option>共享查看</option>
          </select>
        </label>
        <label class="filter-item">
          <span>失败异常类型</span>
          <select v-model="scheduleForm.errorType">
            <option>连接超时</option>
            <option>HTTP 403</option>
            <option>验证码拦截</option>
          </select>
        </label>
        <button class="btn" type="button" @click="previewSchedule">预览裁决</button>
        <button class="btn primary" type="button" @click="acquireNode">申请节点</button>
        <button class="btn" type="button" @click="switchNode">切换节点</button>
        <button class="btn ghost" type="button" @click="releaseNode">释放节点</button>
      </div>
      <p v-if="scheduleMessage" class="panel-message">{{ scheduleMessage }}</p>

      <template v-if="scheduleResult">
        <div v-if="scheduleResult.等待建议" class="advice-box">
          <strong>额度耗尽，等待建议：</strong>
          <ul>
            <li v-for="action in scheduleResult.等待建议.actions" :key="action">{{ action }}</li>
          </ul>
        </div>
        <table class="data-table">
          <thead>
            <tr>
              <th>顺序</th>
              <th>节点编号</th>
              <th>状态</th>
              <th>评分</th>
              <th>延迟(ms)</th>
              <th>近期可用率</th>
              <th>历史可用率</th>
              <th>剩余冷却(秒)</th>
              <th>说明</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(item, index) in scheduleResult.节点顺序" :key="item.节点编号">
              <td>{{ index + 1 }}</td>
              <td>{{ item.节点编号 }}</td>
              <td>{{ item.状态 }}</td>
              <td>{{ item.评分 }}</td>
              <td>{{ item.响应延迟ms }}</td>
              <td>{{ item.近期可用率 }}</td>
              <td>{{ item.历史可用率 }}</td>
              <td>{{ item.剩余冷却秒数 }}</td>
              <td>{{ item.说明 }}</td>
            </tr>
          </tbody>
        </table>
      </template>

      <h4>待等待清单</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>位次</th>
            <th>计划编号</th>
            <th>目标站点</th>
            <th>优先级</th>
            <th>操作人</th>
            <th>入队时间</th>
            <th>预计等待(秒)</th>
            <th>排队原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in queueItems" :key="String(item.计划编号)">
            <td>{{ item.排队位次 }}</td>
            <td>{{ item.计划编号 }}</td>
            <td>{{ item.目标站点 }}</td>
            <td>{{ item.优先级 }}</td>
            <td>{{ item.操作人 }}</td>
            <td>{{ item.入队时间 }}</td>
            <td>{{ item.预计等待秒数 ?? '待释放' }}</td>
            <td>{{ item.排队原因 }}</td>
          </tr>
          <tr v-if="!queueItems.length">
            <td colspan="8" class="empty-state">当前没有排队计划</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条代理池管理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/proxy'
const columns = ["节点编号", "代理地址", "代理类型", "地理位置", "响应延迟", "可用率", "切换策略", "节点状态", "服务站点", "归属负责人"]
const actions = ["提交校验", "启用节点", "停用节点"]
const stats = [{"label": "代理节点总数", "value": 0}, {"label": "待校验节点", "value": 0}, {"label": "已启用节点", "value": 0}]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const plans = ref<Row[]>([])
const queueItems = ref<Row[]>([])
const scheduleResult = ref<Record<string, any> | null>(null)
const scheduleMessage = ref('')
const scheduleForm = reactive({
  planId: 1,
  operator: '张倩',
  role: '负责人',
  errorType: '连接超时',
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '代理节点登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '代理池管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理池管理操作失败'
  }
}

async function postControlled(path: string, values: Record<string, unknown>) {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  const payload = await response.json()
  scheduleMessage.value = payload.message || ''
  if (!payload.ok) {
    errorMessage.value = payload.message || '操作被驳回'
  } else {
    errorMessage.value = ''
  }
  await Promise.all([reload(), loadQueue()])
  return payload
}

async function previewSchedule() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/schedule?plan_id=${scheduleForm.planId}`)
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '裁决预览失败')
    }
    scheduleResult.value = payload
    scheduleMessage.value = payload.额度耗尽 ? '额度耗尽，详见等待建议' : `可执行节点顺序：${payload.可执行节点.join(' → ') || '无'}`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '裁决预览失败'
  }
}

async function acquireNode() {
  const payload = await postControlled(`${ENDPOINT}/acquire`, {
    plan_id: scheduleForm.planId,
    operator: scheduleForm.operator,
    role: scheduleForm.role,
  })
  if (payload.裁决) {
    scheduleResult.value = payload.裁决
  }
}

async function switchNode() {
  const payload = await postControlled(`${ENDPOINT}/switch`, {
    plan_id: scheduleForm.planId,
    operator: scheduleForm.operator,
    role: scheduleForm.role,
  })
  if (payload.裁决) {
    scheduleResult.value = payload.裁决
  }
}

async function releaseNode() {
  await postControlled(`${ENDPOINT}/release`, {
    plan_id: scheduleForm.planId,
    operator: scheduleForm.operator,
    role: scheduleForm.role,
  })
}

async function reportNode(row: Row, result: 'success' | 'failure') {
  await postControlled(`${ENDPOINT}/${row.id}/report`, {
    result,
    latency_ms: 200,
    异常类型: scheduleForm.errorType,
    operator: scheduleForm.operator,
    role: scheduleForm.role,
  })
}

async function healthCheck(row: Row) {
  await postControlled(`${ENDPOINT}/${row.id}/health-check`, {
    passed: true,
    operator: scheduleForm.operator,
    role: scheduleForm.role,
  })
}

async function loadPlans() {
  try {
    const response = await request('/api/taskplan?size=100')
    const payload = await response.json()
    plans.value = payload.items ?? []
    if (plans.value.length && !plans.value.some((plan) => Number(plan.id) === scheduleForm.planId)) {
      scheduleForm.planId = Number(plans.value[0].id)
    }
  } catch {
    plans.value = []
  }
}

async function loadQueue() {
  try {
    const response = await request(`${ENDPOINT}/queue`)
    const payload = await response.json()
    queueItems.value = payload.items ?? []
  } catch {
    queueItems.value = []
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('代理节点列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理池管理列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadPlans()
  void loadQueue()
})
</script>
