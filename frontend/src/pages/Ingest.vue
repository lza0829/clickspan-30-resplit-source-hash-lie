<script setup>
import { onMounted, ref } from 'vue'
import { api } from '../api.js'

const result = ref(null)
const err = ref('')
const busy = ref(false)
const pending = ref([])
const stagedBatchId = ref('')

async function refresh() {
  try {
    const r = await api.ingestCandidates()
    pending.value = r.pending
  } catch (e) {
    err.value = String(e.message || e)
  }
}

onMounted(refresh)

async function run() {
  busy.value = true
  err.value = ''
  result.value = null
  try {
    result.value = await api.ingest()
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function stage() {
  busy.value = true
  err.value = ''
  result.value = null
  try {
    const r = await api.ingestStage()
    stagedBatchId.value = r.batch_id
    result.value = r
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function commit() {
  busy.value = true
  err.value = ''
  result.value = null
  try {
    result.value = await api.ingestCommit(stagedBatchId.value)
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function abort() {
  busy.value = true
  err.value = ''
  result.value = null
  try {
    result.value = await api.ingestAbort(stagedBatchId.value)
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <!-- plant-ui -->

  <section class="panel">
    <h1>收流击点</h1>
    <p class="muted">
      读取夹具 ingest_batch.json：可一批原子灌入（挂候选 + 提交装载后），
      也可只挂候选演示交错写——候选未提交期间，重切会提交。
    </p>
    <p>
      <button class="btn" :disabled="busy" @click="run">{{ busy ? '收流中…' : '跑一批收流' }}</button>
      <button class="btn" :disabled="busy" @click="stage">只挂候选（不提交）</button>
      <button class="btn" :disabled="busy || !pending.length" @click="commit">提交候选</button>
      <button class="btn" :disabled="busy || !pending.length" @click="abort">作废候选</button>
    </p>
    <p v-if="pending.length" class="muted">
      未提交候选 {{ pending.length }} 个：
      <span v-for="p in pending" :key="p.id" class="mono">{{ p.id }}({{ p.visitor_id }}) </span>
      —— 此时触发重切将整笔拒绝并回滚
    </p>
    <p v-if="err" class="err">{{ err }}</p>
    <pre v-if="result" class="mono">{{ result }}</pre>
  </section>
</template>
