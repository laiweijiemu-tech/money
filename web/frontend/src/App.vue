<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { BarChart, LineChart, GaugeChart, type BarSeriesOption, type LineSeriesOption, type GaugeSeriesOption } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  type GridComponentOption,
  type TooltipComponentOption,
  type LegendComponentOption,
} from 'echarts/components'
import { use, init, graphic, type ComposeOption, type EChartsType } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'

use([BarChart, LineChart, GaugeChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

type ChartOption = ComposeOption<
  BarSeriesOption | LineSeriesOption | GaugeSeriesOption | GridComponentOption | TooltipComponentOption | LegendComponentOption
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
  rising_count: number
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

interface HotBoard {
  name: string
  avg_change: number
  count: number
  leader_name: string
}

interface DashboardData {
  generated_at: string
  overview: Overview
  emotion_trend: EmotionPoint[]
  themes: ThemeItem[]
  hot_boards: HotBoard[]
  leaders: LeaderItem[]
  alerts: AlertItem[]
  positions: PositionItem[]
  source: SourceInfo
}

type TabName = 'leader' | 'hotspot' | 'emotion' | 'position'

const dashboard = ref<DashboardData | null>(null)
const loading = ref(true)
const errorMessage = ref('')
const activeTab = ref<TabName>('leader')

const themeChartRef = ref<HTMLDivElement | null>(null)
const emotionChartRef = ref<HTMLDivElement | null>(null)
const gaugeChartRef = ref<HTMLDivElement | null>(null)
const emotionBarChartRef = ref<HTMLDivElement | null>(null)

let themeChart: EChartsType | null = null
let emotionChart: EChartsType | null = null
let gaugeChart: EChartsType | null = null
let emotionBarChart: EChartsType | null = null
let refreshTimer: number | null = null

const tabs: { key: TabName; label: string }[] = [
  { key: 'leader', label: '龙头战法' },
  { key: 'hotspot', label: '热点挖掘' },
  { key: 'emotion', label: '情绪看板' },
  { key: 'position', label: '持仓风控' },
]

const statCards = computed(() => {
  if (!dashboard.value) return []
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
const expandedTheme = ref<string | null>(null)

function toggleTheme(name: string): void {
  expandedTheme.value = expandedTheme.value === name ? null : name
}

function themeStocks(themeName: string): LeaderItem[] {
  return (dashboard.value?.leaders ?? []).filter((s) => s.theme === themeName)
}

const sourceBadge = computed(() => {
  if (!dashboard.value) return '数据同步中'
  return dashboard.value.source.is_live ? '实时数据' : '本地快照'
})

const formattedUpdateTime = computed(() => {
  if (!dashboard.value) return '--'
  return dashboard.value.generated_at.replace('T', ' ')
})

const sentimentDescription = computed(() => {
  const score = dashboard.value?.overview.market_sentiment ?? 0
  if (score >= 75) return '情绪偏强，可聚焦主线核心与分歧回封机会'
  if (score >= 55) return '情绪中性偏暖，宜优先做板块内辨识度龙头'
  return '情绪偏弱，注意控制出手频率与仓位'
})

const sentimentLevel = computed(() => {
  const score = dashboard.value?.overview.market_sentiment ?? 0
  if (score >= 75) return 'high'
  if (score >= 55) return 'mid'
  return 'low'
})

const totalProfit = computed(() => {
  const positions = dashboard.value?.positions ?? []
  if (!positions.length) return 0
  return positions.reduce((sum, p) => sum + p.profit_pct, 0) / positions.length
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

function switchTab(tab: TabName): void {
  activeTab.value = tab
  nextTick(() => renderCharts())
}

async function fetchDashboard(): Promise<void> {
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await fetch('/api/dashboard')
    if (!response.ok) throw new Error('仪表盘数据获取失败')
    dashboard.value = (await response.json()) as DashboardData
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '网络异常，请稍后重试'
  } finally {
    loading.value = false
  }
}

function renderThemeChart(data: DashboardData): void {
  if (!themeChartRef.value) return
  const boards = data.hot_boards ?? []
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
      data: boards.map((item) => item.name).reverse(),
      axisLabel: { color: '#cbd5e1' },
      axisTick: { show: false },
      axisLine: { show: false },
    },
    series: [
      {
        type: 'bar',
        data: boards.map((item) => item.avg_change).reverse(),
        barWidth: 16,
        itemStyle: {
          borderRadius: 8,
          color: (params: any) => {
            const val = params.data as number
            if (val >= 2) return new graphic.LinearGradient(1, 0, 0, 0, [{ offset: 0, color: '#ef4444' }, { offset: 1, color: '#f97316' }])
            if (val >= 1) return new graphic.LinearGradient(1, 0, 0, 0, [{ offset: 0, color: '#22c55e' }, { offset: 1, color: '#38bdf8' }])
            return new graphic.LinearGradient(1, 0, 0, 0, [{ offset: 0, color: '#94a3b8' }, { offset: 1, color: '#cbd5e1' }])
          },
        },
      },
    ],
  }
  themeChart ??= init(themeChartRef.value)
  themeChart.setOption(option)
}

function renderEmotionChart(data: DashboardData): void {
  if (!emotionChartRef.value) return
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

function renderGaugeChart(data: DashboardData): void {
  if (!gaugeChartRef.value) return
  const score = data.overview.market_sentiment
  const option: ChartOption = {
    series: [
      {
        type: 'gauge',
        startAngle: 200,
        endAngle: -20,
        min: 0,
        max: 100,
        pointer: { show: true, length: '60%', width: 5 },
        axisLine: {
          lineStyle: {
            width: 20,
            color: [
              [0.4, '#ef4444'],
              [0.6, '#f59e0b'],
              [0.8, '#22c55e'],
              [1, '#06b6d4'],
            ],
          },
        },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
        detail: {
          fontSize: 28,
          fontWeight: 'bold',
          offsetCenter: [0, '70%'],
          formatter: '{value}',
          color: '#1e293b',
        },
        data: [{ value: score }],
      },
    ],
  }
  gaugeChart ??= init(gaugeChartRef.value)
  gaugeChart.setOption(option)
}

function renderEmotionBarChart(data: DashboardData): void {
  if (!emotionBarChartRef.value) return
  const option: ChartOption = {
    tooltip: { trigger: 'axis' },
    grid: { left: 12, right: 12, top: 24, bottom: 8, containLabel: true },
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
        type: 'bar',
        data: data.emotion_trend.map((item) => item.value),
        barWidth: 24,
        itemStyle: {
          borderRadius: 6,
          color: (params: any) => {
            const val = params.data as number
            if (val >= 75) return '#22c55e'
            if (val >= 55) return '#f59e0b'
            return '#ef4444'
          },
        },
      },
      {
        type: 'line',
        smooth: true,
        data: data.emotion_trend.map((item) => item.value),
        lineStyle: { width: 2, color: '#6366f1', type: 'dashed' },
        itemStyle: { color: '#6366f1' },
        symbol: 'circle',
        symbolSize: 6,
      },
    ],
  }
  emotionBarChart ??= init(emotionBarChartRef.value)
  emotionBarChart.setOption(option)
}

function renderCharts(): void {
  if (!dashboard.value) return
  if (activeTab.value === 'hotspot') {
    themeChart?.dispose()
    themeChart = null
    emotionChart?.dispose()
    emotionChart = null
    renderThemeChart(dashboard.value)
    renderEmotionChart(dashboard.value)
  }
  if (activeTab.value === 'emotion') {
    gaugeChart?.dispose()
    gaugeChart = null
    emotionBarChart?.dispose()
    emotionBarChart = null
    renderGaugeChart(dashboard.value)
    renderEmotionBarChart(dashboard.value)
  }
}

function handleResize(): void {
  themeChart?.resize()
  emotionChart?.resize()
  gaugeChart?.resize()
  emotionBarChart?.resize()
}

watch(
  dashboard,
  async (value) => {
    if (!value) return
    await nextTick()
    renderCharts()
  },
  { deep: true },
)

onMounted(async () => {
  await fetchDashboard()
  window.addEventListener('resize', handleResize)
  refreshTimer = window.setInterval(() => { void fetchDashboard() }, 15000)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (refreshTimer !== null) window.clearInterval(refreshTimer)
  themeChart?.dispose()
  emotionChart?.dispose()
  gaugeChart?.dispose()
  emotionBarChart?.dispose()
})
</script>

<template>
  <div class="page-shell">
    <header class="main-header">
      <div class="header-left">
        <nav class="main-nav">
          <ul>
            <li
              v-for="tab in tabs"
              :key="tab.key"
              class="nav-item"
              :class="{ active: activeTab === tab.key }"
              @click="switchTab(tab.key)"
            >{{ tab.label }}</li>
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

    <!-- ==================== 龙头战法 ==================== -->
    <template v-if="activeTab === 'leader'">
      <section class="stats-grid">
        <article v-for="card in statCards" :key="card.label" class="stat-card" :data-tone="card.tone">
          <span>{{ card.label }}</span>
          <strong>{{ card.value }}</strong>
        </article>
      </section>

      <section class="main-grid">
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
    </template>

    <!-- ==================== 热点挖掘 ==================== -->
    <template v-if="activeTab === 'hotspot'">
      <section class="main-grid">
        <article class="panel panel-large">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">热点挖掘</p>
              <h2>概念板块涨幅排行</h2>
            </div>
            <span class="timestamp">更新时间 {{ formattedUpdateTime }}</span>
          </div>
          <div ref="themeChartRef" class="chart"></div>
          <div class="theme-list">
            <div v-for="item in dashboard?.hot_boards ?? []" :key="item.name" class="theme-item clickable" @click="toggleTheme(item.name)">
              <div>
                <strong>{{ item.name }}</strong>
                <span>{{ item.count }} 只成分股 / 龙头: {{ item.leader_name }}</span>
              </div>
              <div class="theme-metrics">
                <span :class="item.avg_change >= 0 ? 'up-text' : 'down-text'">{{ formatSigned(item.avg_change) }}</span>
              </div>
            </div>
            <div v-if="expandedTheme" class="theme-drilldown">
              <div class="drilldown-header">{{ expandedTheme }} 入选龙头池</div>
              <div v-for="stock in themeStocks(expandedTheme)" :key="stock.code" class="drilldown-row">
                <span><strong>{{ stock.name }}</strong> {{ stock.code }}</span>
                <span>{{ stock.tag }}</span>
                <span :class="stock.change_pct >= 0 ? 'up-text' : 'down-text'">{{ formatSigned(stock.change_pct) }}</span>
              </div>
              <p v-if="!themeStocks(expandedTheme).length" class="muted-copy">该板块暂无入选龙头池的标的</p>
            </div>
          </div>
        </article>

        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">情绪参考</p>
              <h2>本周情绪趋势</h2>
            </div>
            <span class="score-pill">{{ dashboard?.overview.market_sentiment ?? '--' }}</span>
          </div>
          <div ref="emotionChartRef" class="chart chart-short"></div>
          <p class="muted-copy">{{ sentimentDescription }}</p>
        </article>

        <article class="panel panel-wide">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">预警动态</p>
              <h2>板块异动信号</h2>
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
      </section>
    </template>

    <!-- ==================== 情绪看板 ==================== -->
    <template v-if="activeTab === 'emotion'">
      <section class="stats-grid">
        <article class="stat-card" data-tone="hot">
          <span>情绪温度</span>
          <strong>{{ dashboard?.overview.market_sentiment ?? '--' }}</strong>
        </article>
        <article class="stat-card" data-tone="up">
          <span>打板晋级率</span>
          <strong>{{ dashboard?.overview.board_success_rate ?? '--' }}%</strong>
        </article>
        <article class="stat-card" data-tone="up">
          <span>涨停溢价</span>
          <strong>{{ dashboard?.overview.limit_up_premium ?? '--' }}%</strong>
        </article>
        <article class="stat-card" data-tone="neutral">
          <span>最高连板</span>
          <strong>{{ dashboard?.overview.highest_board ?? '--' }} 板</strong>
        </article>
        <article class="stat-card" data-tone="theme">
          <span>上涨家数</span>
          <strong>{{ dashboard?.overview.rising_count ?? '--' }} / {{ dashboard?.overview.watch_count ?? '--' }}</strong>
        </article>
        <article class="stat-card" :data-tone="sentimentLevel === 'high' ? 'up' : sentimentLevel === 'mid' ? 'warn' : 'hot'">
          <span>情绪判断</span>
          <strong>{{ sentimentLevel === 'high' ? '偏强' : sentimentLevel === 'mid' ? '中性' : '偏弱' }}</strong>
        </article>
      </section>

      <section class="main-grid">
        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">情绪仪表盘</p>
              <h2>市场温度计</h2>
            </div>
          </div>
          <div ref="gaugeChartRef" class="chart"></div>
          <p class="muted-copy">{{ sentimentDescription }}</p>
        </article>

        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">周度趋势</p>
              <h2>情绪变化走势</h2>
            </div>
          </div>
          <div ref="emotionBarChartRef" class="chart"></div>
          <p class="muted-copy">柱状表示每日情绪分，虚线为趋势参考。</p>
        </article>

        <article class="panel panel-wide">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">情绪指标明细</p>
              <h2>赚钱效应分解</h2>
            </div>
          </div>
          <div class="emotion-detail-grid">
            <div class="emotion-detail-item">
              <span class="emotion-label">主线题材</span>
              <strong>{{ dashboard?.overview.main_theme ?? '--' }}</strong>
              <span class="emotion-sub">当前资金攻击最强方向</span>
            </div>
            <div class="emotion-detail-item">
              <span class="emotion-label">题材热度</span>
              <strong>{{ dashboard?.themes[0]?.heat ?? '--' }}</strong>
              <span class="emotion-sub">综合涨停数、涨幅、扩散度</span>
            </div>
            <div class="emotion-detail-item">
              <span class="emotion-label">打板晋级率</span>
              <strong>{{ dashboard?.overview.board_success_rate ?? '--' }}%</strong>
              <span class="emotion-sub">涨停后次日能续涨的比例</span>
            </div>
            <div class="emotion-detail-item">
              <span class="emotion-label">涨停溢价率</span>
              <strong>{{ dashboard?.overview.limit_up_premium ?? '--' }}%</strong>
              <span class="emotion-sub">昨日涨停股今日平均表现</span>
            </div>
            <div class="emotion-detail-item">
              <span class="emotion-label">最高连板</span>
              <strong>{{ dashboard?.overview.highest_board ?? '--' }} 板</strong>
              <span class="emotion-sub">市场空间高度，越高越活跃</span>
            </div>
            <div class="emotion-detail-item">
              <span class="emotion-label">上涨占比</span>
              <strong>{{ dashboard?.overview.rising_count ?? 0 }} / {{ dashboard?.overview.watch_count ?? 0 }}</strong>
              <span class="emotion-sub">池内标的上涨/总数</span>
            </div>
          </div>
        </article>
      </section>
    </template>

    <!-- ==================== 持仓风控 ==================== -->
    <template v-if="activeTab === 'position'">
      <section class="stats-grid" v-if="dashboard?.positions?.length">
        <article class="stat-card" :data-tone="totalProfit >= 0 ? 'up' : 'hot'">
          <span>综合盈亏</span>
          <strong>{{ formatSigned(totalProfit) }}</strong>
        </article>
        <article class="stat-card" data-tone="neutral">
          <span>持仓数量</span>
          <strong>{{ dashboard?.positions?.length ?? 0 }} 只</strong>
        </article>
        <article class="stat-card" data-tone="warn">
          <span>预警数</span>
          <strong>{{ dashboard?.positions?.filter(p => p.drawdown_pct >= 4).length ?? 0 }} 只</strong>
        </article>
      </section>

      <section class="main-grid">
        <article class="panel panel-wide">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">持仓风控</p>
              <h2>风险哨兵</h2>
            </div>
            <span class="timestamp">动态监控</span>
          </div>
          <div class="position-list" v-if="dashboard?.positions?.length">
            <div v-for="position in dashboard.positions" :key="position.code" class="position-item">
              <div class="position-head">
                <strong>{{ position.name }} <span class="position-code">{{ position.code }}</span></strong>
                <span :class="position.profit_pct >= 0 ? 'up-text' : 'down-text'">{{ formatSigned(position.profit_pct) }}</span>
              </div>
              <div class="position-metrics">
                <span>成本 {{ position.cost_price.toFixed(2) }}</span>
                <span>现价 {{ position.last_price.toFixed(2) }}</span>
                <span :class="position.drawdown_pct >= 4 ? 'down-text' : ''">回撤 {{ position.drawdown_pct.toFixed(1) }}%</span>
              </div>
              <p class="risk-note" :class="{ 'risk-warn': position.drawdown_pct >= 4 }">{{ position.risk_note }}</p>
            </div>
          </div>
          <div v-else class="empty-state">
            <p class="empty-title">当前无持仓标的</p>
            <p class="muted-copy">在龙头战法页选中买入的标的后，将自动出现在此处进行风控监控。</p>
            <p class="muted-copy">监控内容包括：动态止损、回撤预警、炸板离场信号。</p>
          </div>
        </article>

        <article class="panel panel-wide">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">风控规则</p>
              <h2>纪律提醒</h2>
            </div>
          </div>
          <div class="rules-list">
            <div class="rule-item">
              <span class="rule-icon">1</span>
              <div>
                <strong>动态止损线</strong>
                <p>跌破5日线或回撤超过4%，触发减仓信号。</p>
              </div>
            </div>
            <div class="rule-item">
              <span class="rule-icon">2</span>
              <div>
                <strong>炸板离场</strong>
                <p>涨停封单骤减或频繁开板，提示利润回撤风险。</p>
              </div>
            </div>
            <div class="rule-item">
              <span class="rule-icon">3</span>
              <div>
                <strong>浮盈保护</strong>
                <p>盈利超过8%后启用移动止盈，锁定利润。</p>
              </div>
            </div>
            <div class="rule-item">
              <span class="rule-icon">4</span>
              <div>
                <strong>单票上限</strong>
                <p>单只个股仓位不超过总资金30%，分散风险。</p>
              </div>
            </div>
          </div>
        </article>
      </section>
    </template>

    <div v-if="loading" class="loading-mask">正在同步盯盘数据...</div>
  </div>
</template>
