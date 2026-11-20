<script setup>
import { computed, onMounted, ref } from 'vue'
import { api } from '../api.js'

const overview = ref(null)
const sessions = ref([])
const details = ref({})
const err = ref('')

// 织机侧重算：当前 open 会话的正式成员点数（与重切源证明互证的同一口径）
const openMemberN = computed(() =>
  sessions.value
    .filter((s) => s.status === 'open')
    .reduce((n, s) => n + ((details.value[s.id] && details.value[s.id].hits.length) || 0), 0)
)

async function load() {
  err.value = ''
  try {
    overview.value = await api.overview()
    sessions.value = await api.sessions()
    const map = {}
    for (const s of sessions.value) {
      map[s.id] = await api.session(s.id)
    }
    details.value = map
  } catch (e) {
    err.value = String(e.message || e)
  }
}

onMounted(load)
</script>

<template>
  <!-- plant-ui -->

  <div>
    <div class="stats" v-if="overview">
      <div class="stat"><b>{{ overview.visitors }}</b><span>访客</span></div>
      <div class="stat"><b>{{ overview.sessions }}</b><span>会话</span></div>
      <div class="stat"><b>{{ overview.hits_primary }}</b><span>正式击点</span></div>
      <div class="stat"><b>{{ openMemberN }}</b><span>open 成员</span></div>
      <div class="stat"><b>{{ overview.quarantine }}</b><span>隔离</span></div>
      <div class="stat"><b>{{ overview.idle_timeout_sec }}s</b><span>空闲超时</span></div>
    </div>
    <p v-if="err" class="err">{{ err }}</p>

    <section class="panel">
      <h1>会话织机</h1>
      <p class="muted">closed 会话成员已冻结；重切入口在「切窗」页，只重切 open。open 成员数与最近重切源证明互证（见「核证」页）。</p>
      <div class="lane-list">
        <article v-for="s in sessions" :key="s.id" class="lane">
          <div class="lane-head">
            <div>
              <strong>{{ s.id }}</strong>
              <span class="muted"> · {{ s.visitor_id }} · {{ s.status }}</span>
            </div>
            <span class="mono">{{ s.hit_count }} 点 · {{ s.started_at }} → {{ s.ended_at || '…' }}</span>
          </div>
          <div class="beads" v-if="details[s.id]">
            <template v-for="(h, i) in details[s.id].hits" :key="h.id">
              <span v-if="i" class="connector" aria-hidden="true"></span>
              <span class="hit-bead">
                <strong>{{ h.event_name }}</strong>
                <span>{{ h.page_path }}</span>
              </span>
            </template>
          </div>
        </article>
      </div>
    </section>
  </div>
</template>
