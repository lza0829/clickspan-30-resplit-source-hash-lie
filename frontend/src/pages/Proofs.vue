<script setup>
import { onMounted, ref } from 'vue'
import { api } from '../api.js'

const rows = ref([])
const batches = ref([])
const verify = ref(null)
const sourceVerify = ref(null)
const err = ref('')

async function load() {
  err.value = ''
  try {
    rows.value = await api.proofs()
    batches.value = await api.resplitBatches()
    verify.value = await api.resplitVerify()
    sourceVerify.value = await api.resplitSourceVerify()
  } catch (e) {
    err.value = String(e.message || e)
  }
}

onMounted(load)
</script>

<template>
  <!-- plant-ui -->

  <section class="panel">
    <h1>会话核对</h1>
    <p class="muted">会话击点集合与起止须与织机汇总可核。</p>
    <p v-if="err" class="err">{{ err }}</p>
    <table class="data">
      <thead>
        <tr><th>会话</th><th>访客</th><th>点数</th><th>起止</th><th>击点集</th></tr>
      </thead>
      <tbody>
        <tr v-for="r in rows" :key="r.id">
          <td class="mono">{{ r.session_id }}</td>
          <td>{{ r.visitor_id }}</td>
          <td>{{ r.hit_count }}</td>
          <td class="mono">{{ r.started_at }} → {{ r.ended_at }}</td>
          <td class="mono">{{ r.hit_ids_json }}</td>
        </tr>
      </tbody>
    </table>
  </section>

  <section class="panel">
    <h1>重切隔离核对</h1>
    <p class="muted">
      只读核对：closed 成员键未变 / 重切只动 open / 切分边不引用未提交候选 / 回执与织机一致。
      <a href="#" @click.prevent="load">重新核对</a>
    </p>
    <div v-if="verify">
      <p :class="verify.ok ? 'ok' : 'err'">
        {{ verify.ok ? '✓ 全部通过' : '✗ 存在违约' }}
        <span class="muted">
          （committed 批次 {{ verify.resplit_batches }} 个，当前未提交候选 {{ verify.pending_candidates }} 个）
        </span>
      </p>
      <ul>
        <li v-for="c in verify.checks" :key="c.key">
          <span :class="c.ok ? 'ok' : 'err'">{{ c.ok ? '✓' : '✗' }}</span>
          <span class="mono">{{ c.key }}</span>
          <ul v-if="c.violations.length">
            <li v-for="(v, i) in c.violations" :key="i" class="err">{{ v }}</li>
          </ul>
        </li>
      </ul>
    </div>
  </section>

  <section class="panel">
    <h1>重切源核证</h1>
    <p class="muted">
      只读核对：回执点数 = 源证明派生点数 = 织机重算的 open 成员数；committed 证明必带源哈希。
      <a href="#" @click.prevent="load">重新核对</a>
    </p>
    <div v-if="sourceVerify">
      <p :class="sourceVerify.ok ? 'ok' : 'err'">
        {{ sourceVerify.ok ? '✓ 源证明与织机一致' : '✗ 源证明与织机分叉' }}
        <span class="muted">
          （织机重算 open 成员 {{ sourceVerify.open_member_n }} 点，集合哈希 {{ sourceVerify.open_source_hash }}）
        </span>
      </p>
      <p v-if="!sourceVerify.batches.length" class="muted">暂无重切批次。</p>
      <table v-else class="data">
        <thead>
          <tr>
            <th>批次</th><th>状态</th><th>源哈希</th><th>回执点数</th>
            <th>证明派生点数</th><th>open 成员数</th><th>结论</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in sourceVerify.batches" :key="b.batch_id">
            <td class="mono">{{ b.batch_id }}</td>
            <td :class="b.status === 'committed' ? 'ok' : 'err'">{{ b.status }}</td>
            <td class="mono">{{ b.source_hash || '—' }}</td>
            <td class="mono">{{ b.scanned_n }}</td>
            <td class="mono">{{ b.proof_member_n }}</td>
            <td class="mono">{{ b.open_member_n }}</td>
            <td :class="b.ok ? 'ok' : 'err'">{{ b.conclusion }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>

  <section class="panel">
    <h1>重切批次证明</h1>
    <p class="muted">每笔重切试图落一行：扫描范围、扫到的 open/closed、改动与切出的会话 id、源哈希与点数快照（不可变）。</p>
    <p v-if="!batches.length" class="muted">暂无重切批次。</p>
    <table v-else class="data">
      <thead>
        <tr>
          <th>批次</th><th>状态</th><th>超时</th><th>扫描范围</th>
          <th>扫到 open</th><th>扫到 closed</th><th>改动</th><th>切出</th>
          <th>源哈希</th><th>扫描点</th><th>改写点</th><th>时刻</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="b in batches" :key="b.id">
          <td class="mono">{{ b.id }}</td>
          <td :class="b.status === 'committed' ? 'ok' : 'err'">{{ b.status }}</td>
          <td class="mono">{{ b.idle_timeout_sec }}s</td>
          <td class="mono">{{ b.scan_from || '…' }} → {{ b.scan_to || '…' }}</td>
          <td class="mono">{{ b.scanned_open.join(', ') }}</td>
          <td class="mono">{{ b.scanned_closed.join(', ') }}</td>
          <td class="mono">{{ b.changed_sessions.join(', ') }}</td>
          <td class="mono">{{ b.created_sessions.join(', ') }}</td>
          <td class="mono">{{ b.source_hash || '—' }}</td>
          <td class="mono">{{ b.scanned_n }}</td>
          <td class="mono">{{ b.rewritten_n }}</td>
          <td class="mono">{{ b.created_at }}</td>
        </tr>
      </tbody>
    </table>
    <template v-for="b in batches" :key="`reason-${b.id}`">
      <p v-if="b.reject_reason" class="err mono">{{ b.id }}：{{ b.reject_reason }}</p>
    </template>
  </section>
</template>
