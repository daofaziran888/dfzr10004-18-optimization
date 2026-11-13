<template>
  <section class="page" data-module="retry">
    <header class="page-head">
      <div>
        <h2>异常重试管理管理</h2>
        <p class="page-desc">维护异常重试的异常分类、退避算法与降级策略，为采集异常恢复提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记重试规则</button>
        <button class="btn" type="button" @click="exportRows">导出异常重试管理清单</button>
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
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无异常重试管理数据，可先登记重试规则</td>
        </tr>
      </tbody>
    </table>

    <section class="panel">
      <h3>重试协同清单</h3>
      <p class="panel-desc">
        重试规则清单与待等待清单同步登记，所有采集计划共享同一份冷却判断；冷却结束的节点需先通过一次健康校验。
      </p>
      <div class="page-actions">
        <button class="btn" type="button" @click="loadCoordination">刷新协同清单</button>
      </div>

      <h4>重试规则冷却口径</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>规则编号</th>
            <th>异常类型</th>
            <th>冷却秒数</th>
            <th>重试上限</th>
            <th>退避策略</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="rule in coordination.重试规则清单" :key="String(rule.规则编号)">
            <td>{{ rule.规则编号 }}</td>
            <td>{{ rule.异常类型 }}</td>
            <td>{{ rule.冷却秒数 }}</td>
            <td>{{ rule.重试上限 }}</td>
            <td>{{ rule.退避策略 }}</td>
          </tr>
          <tr v-if="!coordination.重试规则清单.length">
            <td colspan="5" class="empty-state">暂无已启用的重试规则</td>
          </tr>
        </tbody>
      </table>

      <h4>共享冷却判断</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>节点编号</th>
            <th>服务站点</th>
            <th>状态</th>
            <th>剩余冷却(秒)</th>
            <th>冷却结束时间</th>
            <th>最近失败时间</th>
            <th>最近异常类型</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="node in coordination.共享冷却判断" :key="String(node.节点编号)">
            <td>{{ node.节点编号 }}</td>
            <td>{{ node.服务站点 ?? '—' }}</td>
            <td>{{ node.状态 }}</td>
            <td>{{ node.剩余冷却秒数 }}</td>
            <td>{{ node.冷却结束时间 ?? '—' }}</td>
            <td>{{ node.最近失败时间 ?? '—' }}</td>
            <td>{{ node.最近异常类型 ?? '—' }}</td>
          </tr>
          <tr v-if="!coordination.共享冷却判断.length">
            <td colspan="7" class="empty-state">暂无代理节点</td>
          </tr>
        </tbody>
      </table>

      <h4>待等待清单</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>位次</th>
            <th>计划编号</th>
            <th>目标站点</th>
            <th>优先级</th>
            <th>预计等待(秒)</th>
            <th>排队原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in coordination.待等待清单" :key="String(item.计划编号)">
            <td>{{ item.排队位次 }}</td>
            <td>{{ item.计划编号 }}</td>
            <td>{{ item.目标站点 }}</td>
            <td>{{ item.优先级 }}</td>
            <td>{{ item.预计等待秒数 ?? '待释放' }}</td>
            <td>{{ item.排队原因 }}</td>
          </tr>
          <tr v-if="!coordination.待等待清单.length">
            <td colspan="6" class="empty-state">当前没有排队计划</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条异常重试管理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/retry'
const columns = ["规则编号", "异常类型", "重试上限", "退避策略", "冷却时间", "告警阈值", "降级方案", "是否启用"]
const actions = ["提交校验", "启用规则", "停用规则"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const stats = [{"label": "重试规则总数", "value": 0}, {"label": "待校验规则", "value": 0}, {"label": "已启用规则", "value": 0}]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

type Coordination = {
  重试规则清单: Row[]
  共享冷却判断: Row[]
  待等待清单: Row[]
}

const coordination = ref<Coordination>({ 重试规则清单: [], 共享冷却判断: [], 待等待清单: [] })

async function loadCoordination() {
  try {
    const response = await request(`${ENDPOINT}/coordination`)
    const payload = await response.json()
    coordination.value = {
      重试规则清单: payload.重试规则清单 ?? [],
      共享冷却判断: payload.共享冷却判断 ?? [],
      待等待清单: payload.待等待清单 ?? [],
    }
  } catch {
    coordination.value = { 重试规则清单: [], 共享冷却判断: [], 待等待清单: [] }
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '重试规则登记入口尚未接入审批流'
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
      throw new Error(payload.message || '异常重试管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异常重试管理操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('重试规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异常重试管理列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadCoordination()
})
</script>
