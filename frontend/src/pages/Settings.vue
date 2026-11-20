<script setup>
import { onMounted, ref } from 'vue'
import { api } from '../api.js'

const sec = ref(1800)
const msg = ref('')
const err = ref('')
const receipt = ref(null)
const busy = ref(false)

onMounted(async () => {
  try {
    const s = await api.settings()
    sec.value = s.idle_timeout_sec
  } catch (e) {
    err.value = String(e.message || e)
  }
})

async function save() {
  err.value = ''
  msg.value = ''
  try {
    const res = await fetch('/api/settings/idle-timeout', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ idle_timeout_sec: Number(sec.value) }),
    })
    if (!res.ok) throw new Error(await res.text())
    const data = await res.json()
    sec.value = data.idle_timeout_sec
    msg.value = '已保存（重切按当时现行超时执行）'
  } catch (e) {
    err.value = String(e.message || e)
  }
}

async function resplit() {
  busy.value = true
  err.value = ''
  msg.value = ''
  receipt.value = null
  try {
    const r = await api.resplitRun({})
    receipt.value = r
    if (r.ok) {
      msg.value = `重切完成 ${r.batch_id}：扫描 open 成员 ${r.scanned_n} 点（源哈希 ${r.source_hash}），改写 ${r.rewritten_n} 点，改动 ${r.changed_sessions.length} 个 open，切出 ${r.created_sessions.length} 个新会话`
    } else {
      err.value = `重切被拒（已整笔回滚）：${r.reason}`
    }
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <section class="panel">
    <h1>空闲切窗</h1>
    <p class="muted">配置写入 settings；重切按此处现行超时切 open 会话。</p>
    <label>
      秒数
      <input v-model.number="sec" type="number" min="60" step="60" class="mono" />
    </label>
    <p>
      <button class="btn" @click="save">保存</button>
    </p>
    <p v-if="msg" class="ok">{{ msg }}</p>
    <p v-if="err" class="err">{{ err }}</p>
  </section>

  <section class="panel">
    <h1>重切 open 会话</h1>
    <p class="muted">
      按现行超时只重切 open 会话；closed 成员与既有切分边冻结，
      扫到未提交收流候选即整笔拒绝并回滚。批次证明与核证见「核证」页。
    </p>
    <p>
      <button class="btn" :disabled="busy" @click="resplit">
        {{ busy ? '重切中…' : '按现行超时重切' }}
      </button>
    </p>
    <pre v-if="receipt" class="mono">{{ receipt }}</pre>
  </section>
</template>
