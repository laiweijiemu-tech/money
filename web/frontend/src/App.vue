<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { BarChart, LineChart, type BarSeriesOption, type LineSeriesOption } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  type GridComponentOption,
  type TooltipComponentOption,
} from 'echarts/components'
import { use, init, graphic, type ComposeOption, type EChartsType } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'

use([BarChart, LineChart, GridComponent, TooltipComponent, CanvasRenderer])

type ChartOption = ComposeOption<
  BarSeriesOption | LineSeriesOption | GridComponentOption | TooltipComponentOption
>

interface Overview {
  trading_phase: string
  main_theme: string
  market_sentiment: number
  watch_count: number
  alert_count: number
  board_success_rate: number
  limit_up_premium: number
  highest_board: number
}

interface EmotionPoint {
  label: string
  value: number
}

interface ThemeItem {
  name: string
  heat: number
  change_pct: number
  leaders: number
}

interface LeaderItem {
  code: string
  name: string
  theme: string
  tag: string
  last_price: number
  change_pct: number
  turnover_rate: number
  seal_amount: number
  market_cap: number
  status: string
}

interface AlertItem {
  title: string
  stock_name: string
  stock_code: string
  level: 'high' | 'medium' | 'low'
  message: string
  triggered_at: string
}

interface PositionItem {
  code: string
  name: string
  cost_price: number
  last_price: number
  profit_pct: number
  drawdown_pct: number
  risk_note: string
}

interface SourceInfo {
  provider: string
  mode: string
  is_live: boolean
  message: string
}

interface DashboardData {
  generated_at: string
  overview: Overview
  emotion_trend: EmotionPoint[]
  themes: ThemeItem[]
  leaders: LeaderItem[]
  alerts: AlertItem[]
  positions: PositionItem[]
  source: SourceInfo
}

const dashboard = ref<DashboardData | null>(null)
const loading = ref(true)
const errorMessage = ref('')

const themeChartRef = ref<HTMLDivElement | null>(null)
const emotionChartRef = ref<HTMLDivElement | null>(null)

let themeChart: EChartsType | null = null
let emotionChart: EChartsType | null = null
let refreshTimer: number | null = null

const statCards = computed(() => {
  if (!dashboard.value) {
    return []
  }
  const { overview } = dashboard.value
  return [
    { label: '主线题材', value: overview.main_theme, tone: 'theme' },
    { label: '情绪温度', value: `${overview.market_sentiment}`, tone: 'hot' },
    { label: '盯盘标的', value: `${overview.watch_count} 只`, tone: 'neutral' },
    { label: '预警次数', value: `${overview.alert_count} 次`, tone: 'warn' },
    { label: '打板晋级率', value: `${overview.board_success_rate}%`, tone: 'up' },
    { label: '昨日涨停溢价', value: `${overview.limit_up_premium}%`, tone: 'up' },
  ]
})

const topLeader = computed(() => dashboard.value?.leaders[0] ?? null)
const sourceBadge = computed(() => {
  if (!dashboard.value) {
    return '数据同步中'
  }
  return dashboard.value.source.is_live ? '实时数据' : '本地快照'
})
const formattedUpdateTime = computed(() => {
  if (!dashboard.value) {
    return '--'
  }
  return dashboard.value.generated_at.replace('T', ' ')
})

const sentimentDescription = computed(() => {
  const score = dashboard.value?.overview.market_sentiment ?? 0
  if (score >= 75) {
    return '情绪偏强，可聚焦主线核心与分歧回封机会'
  }
  if (score >= 55) {
    return '情绪中性偏暖，宜优先做板块内辨识度龙头'
  }
  return '情绪偏弱，注意控制出手频率与仓位'
})

function formatSigned(value: number): string {
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`
}

function formatSealAmount(value: number): string {
  return `${value.toFixed(2)} 亿`
}

function alertLevelClass(level: AlertItem['level']): string {
  return `level-${level}`
}

async function fetchDashboard(): Promise<void> {
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await fetch('/api/dashboard')
    if (!response.ok) {
      throw new Error('仪表盘数据获取失败')
    }
    const result = (await response.json()) as DashboardData
    dashboard.value = result
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '网络异常，请稍后重试'
  } finally {
    loading.value = false
  }
}

function renderThemeChart(data: DashboardData): void {
  if (!themeChartRef.value) {
    return
  }
  const option: ChartOption = {
    tooltip: { trigger: 'axis' },
    grid: { left: 12, right: 12, top: 24, bottom: 8, containLabel: true },
    xAxis: {
      type: 'value',
      axisLabel: { color: '#94a3b8' },
      splitLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.15)' } },
    },
    yAxis: {
      type: 'category',
      data: data.themes.map((item) => item.name),
      axisLabel: { color: '#cbd5e1' },
      axisTick: { show: false },
      axisLine: { show: false },
    },
    series: [
      {
        type: 'bar',
        data: data.themes.map((item) => item.heat),
        barWidth: 16,
        itemStyle: {
          borderRadius: 8,
          color: new graphic.LinearGradient(1, 0, 0, 0, [
            { offset: 0, color: '#22c55e' },
            { offset: 1, color: '#38bdf8' },
          ]),
        },
      },
    ],
  }
  themeChart ??= init(themeChartRef.value)
  themeChart.setOption(option)
}

function renderEmotionChart(data: DashboardData): void {
  if (!emotionChartRef.value) {
    return
  }
  const option: ChartOption = {
    tooltip: { trigger: 'axis' },
    grid: { left: 8, right: 8, top: 24, bottom: 8, containLabel: true },
    xAxis: {
      type: 'category',
      data: data.emotion_trend.map((item) => item.label),
      axisLabel: { color: '#94a3b8' },
      axisLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.2)' } },
    },
    yAxis: {
      type: 'value',
      min: 0,
      max: 100,
      axisLabel: { color: '#94a3b8' },
      splitLine: { lineStyle: { color: 'rgba(148, 163, 184, 0.15)' } },
    },
    series: [
      {
        type: 'line',
        smooth: true,
        data: data.emotion_trend.map((item) => item.value),
        lineStyle: { width: 3, color: '#f97316' },
        itemStyle: { color: '#fb7185' },
        areaStyle: {
          color: new graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(249, 115, 22, 0.4)' },
            { offset: 1, color: 'rgba(249, 115, 22, 0.02)' },
          ]),
        },
      },
    ],
  }
  emotionChart ??= init(emotionChartRef.value)
  emotionChart.setOption(option)
}

function renderCharts(): void {
  if (!dashboard.value) {
    return
  }
  renderThemeChart(dashboard.value)
  renderEmotionChart(dashboard.value)
}

function handleResize(): void {
  themeChart?.resize()
  emotionChart?.resize()
}

watch(
  dashboard,
  async (value) => {
    if (!value) {
      return
    }
    await nextTick()
    renderCharts()
  },
  { deep: true },
)

onMounted(async () => {
  await fetchDashboard()
  window.addEventListener('resize', handleResize)
  refreshTimer = window.setInterval(() => {
    void fetchDashboard()
  }, 15000)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (refreshTimer !== null) {
    window.clearInterval(refreshTimer)
  }
  themeChart?.dispose()
  emotionChart?.dispose()
})
</script>

<template>
  <div class="page-shell">
    <header class="main-header">
      <div class="header-left">
        <nav class="main-nav">
          <ul>
            <li class="nav-item active">龙头战法</li>
            <li class="nav-item">热点挖掘</li>
            <li class="nav-item">情绪看板</li>
            <li class="nav-item">持仓风控</li>
          </ul>
        </nav>
      </div>
      <div class="header-right">
        <div class="header-status">
          <span>{{ dashboard?.overview.trading_phase ?? '数据加载中' }}</span>
          <span class="status-dot"></span>
          <span>{{ sourceBadge }}</span>
        </div>
        <button class="refresh-button" type="button" @click="fetchDashboard">刷新数据</button>
      </div>
    </header>

    <section v-if="dashboard?.source" class="feedback-card source-card">
      <strong>{{ dashboard.source.provider }}</strong>
      <p>{{ dashboard.source.message }}</p>
    </section>

    <section v-if="errorMessage" class="feedback-card error-card">
      <strong>数据连接异常</strong>
      <p>{{ errorMessage }}</p>
    </section>

    <section class="stats-grid">
      <article
        v-for="card in statCards"
        :key="card.label"
        class="stat-card"
        :data-tone="card.tone"
      >
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
      </article>
    </section>

    <section class="main-grid">
      <article class="panel panel-large">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">热点挖掘</p>
            <h2>主线题材热度榜</h2>
          </div>
          <span class="timestamp">更新时间 {{ formattedUpdateTime }}</span>
        </div>
        <div ref="themeChartRef" class="chart"></div>
        <div class="theme-list">
          <div v-for="item in dashboard?.themes ?? []" :key="item.name" class="theme-item">
            <div>
              <strong>{{ item.name }}</strong>
              <span>{{ item.leaders }} 只涨停 / 题材扩散中</span>
            </div>
            <div class="theme-metrics">
              <span>{{ item.heat }} 热度</span>
              <span class="up-text">{{ formatSigned(item.change_pct) }}</span>
            </div>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">情绪仪表盘</p>
            <h2>市场情绪温度</h2>
          </div>
          <span class="score-pill">{{ dashboard?.overview.market_sentiment ?? '--' }}</span>
        </div>
        <div ref="emotionChartRef" class="chart chart-short"></div>
        <p class="muted-copy">{{ sentimentDescription }}</p>
      </article>

      <article class="panel panel-wide">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">龙头选股池</p>
            <h2>核心标的跟踪</h2>
          </div>
          <span class="timestamp">主盯 {{ dashboard?.overview.watch_count ?? 0 }} 只</span>
        </div>
        <div class="leaders-table">
          <div class="table-head">
            <span>股票</span>
            <span>题材</span>
            <span>状态</span>
            <span>涨幅</span>
            <span>换手</span>
            <span>市值</span>
            <span>封单</span>
          </div>
          <div v-for="stock in dashboard?.leaders ?? []" :key="stock.code" class="table-row">
            <div>
              <strong>{{ stock.name }}</strong>
              <span>{{ stock.code }} / {{ stock.tag }}</span>
            </div>
            <span>{{ stock.theme }}</span>
            <span>{{ stock.status }}</span>
            <span :class="stock.change_pct >= 0 ? 'up-text' : 'down-text'">{{ formatSigned(stock.change_pct) }}</span>
            <span>{{ stock.turnover_rate.toFixed(1) }}%</span>
            <span>{{ stock.market_cap?.toFixed(1) ?? '--' }} 亿</span>
            <span>{{ formatSealAmount(stock.seal_amount) }}</span>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">战法信号</p>
            <h2>盘中雷达预警</h2>
          </div>
          <span class="timestamp">{{ dashboard?.alerts.length ?? 0 }} 条</span>
        </div>
        <div class="alert-list">
          <div v-for="alert in dashboard?.alerts ?? []" :key="`${alert.stock_code}-${alert.triggered_at}`" class="alert-item">
            <span class="alert-level" :class="alertLevelClass(alert.level)">{{ alert.title }}</span>
            <strong>{{ alert.stock_name }} {{ alert.stock_code }}</strong>
            <p>{{ alert.message }}</p>
            <time>{{ alert.triggered_at }}</time>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">持仓风控</p>
            <h2>风险哨兵</h2>
          </div>
          <span class="timestamp">动态监控</span>
        </div>
        <div class="position-list">
          <div v-for="position in dashboard?.positions ?? []" :key="position.code" class="position-item">
            <div class="position-head">
              <strong>{{ position.name }}</strong>
              <span :class="position.profit_pct >= 0 ? 'up-text' : 'down-text'">{{ formatSigned(position.profit_pct) }}</span>
            </div>
            <div class="position-metrics">
              <span>成本 {{ position.cost_price.toFixed(2) }}</span>
              <span>现价 {{ position.last_price.toFixed(2) }}</span>
              <span>回撤 {{ position.drawdown_pct.toFixed(1) }}%</span>
            </div>
            <p>{{ position.risk_note }}</p>
          </div>
        </div>
      </article>

      <article v-if="topLeader" class="panel panel-wide focus-card">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">今日焦点</p>
            <h2>{{ topLeader.name }} {{ topLeader.code }}</h2>
          </div>
          <span class="score-pill">{{ topLeader.tag }}</span>
        </div>
        <div class="focus-grid">
          <div>
            <span>所属题材</span>
            <strong>{{ topLeader.theme }}</strong>
          </div>
          <div>
            <span>最新价格</span>
            <strong>{{ topLeader.last_price.toFixed(2) }}</strong>
          </div>
          <div>
            <span>涨跌幅</span>
            <strong class="up-text">{{ formatSigned(topLeader.change_pct) }}</strong>
          </div>
          <div>
            <span>封单金额</span>
            <strong>{{ formatSealAmount(topLeader.seal_amount) }}</strong>
          </div>
        </div>
      </article>
    </section>

    <div v-if="loading" class="loading-mask">正在同步盯盘数据...</div>
  </div>
</template>
