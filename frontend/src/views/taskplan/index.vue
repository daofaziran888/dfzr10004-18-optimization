<template>
  <section class="page" data-module="taskplan">
    <header class="page-head">
      <div>
        <h2>采集任务编排管理</h2>
        <p class="page-desc">维护采集计划的站点配置、频率设置与资源分配，为采集任务编排提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记采集计划</button>
        <button class="btn" type="button" @click="exportRows">导出采集任务编排清单</button>
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
          <td :colspan="columns.length + 1" class="empty-state">暂无采集任务编排数据，可先登记采集计划</td>
        </tr>
      </tbody>
    </table>

    <section class="panel">
      <h3>计划待办队列</h3>
      <p class="panel-desc">
        待办队列跟着代理调度裁决重排：在租执行、可立即执行、排队中、等待冷却共用同一份冷却判断。
      </p>
      <div class="page-actions">
        <button class="btn" type="button" @click="loadTodo">刷新待办队列</button>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>计划编号</th>
            <th>目标站点</th>
            <th>优先级</th>
            <th>调度状态</th>
            <th>建议节点</th>
            <th>预计等待(秒)</th>
            <th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in todoItems" :key="String(item.计划编号)">
            <td>{{ item.计划编号 }}</td>
            <td>{{ item.目标站点 }}</td>
            <td>{{ item.优先级 }}</td>
            <td>{{ item.调度状态 }}</td>
            <td>{{ item.建议节点 ?? '—' }}</td>
            <td>{{ item.预计等待秒数 ?? '待释放' }}</td>
            <td>{{ item.说明 }}</td>
          </tr>
          <tr v-if="!todoItems.length">
            <td colspan="7" class="empty-state">暂无待办计划</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集任务编排记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/taskplan'
const columns = ["计划编号", "目标站点", "采集频率", "数据范围", "优先级", "资源配额", "预计用时", "计划状态"]
const actions = ["启动计划", "暂停计划", "归档计划"]
const statuses = ["草稿", "运行中", "已暂停", "已完成"]
const stats = [{"label": "采集计划总数", "value": 0}, {"label": "运行中计划", "value": 0}, {"label": "已暂停计划", "value": 0}]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const todoItems = ref<Row[]>([])

async function loadTodo() {
  try {
    const response = await request(`${ENDPOINT}/todo`)
    const payload = await response.json()
    todoItems.value = payload.items ?? []
  } catch {
    todoItems.value = []
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
  errorMessage.value = '采集计划登记入口尚未接入审批流'
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
      throw new Error(payload.message || '采集任务编排动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集任务编排操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('采集计划列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集任务编排列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadTodo()
})
</script>
