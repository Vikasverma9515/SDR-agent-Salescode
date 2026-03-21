import { useEffect, useRef, useState, useCallback } from 'react'

export default function LogStream({ threadId, onEvent }) {
  const [logs, setLogs] = useState([])
  const [connected, setConnected] = useState(false)
  const wsRef = useRef(null)
  const endRef = useRef(null)
  const doneRef = useRef(false)
  const onEventRef = useRef(onEvent)
  onEventRef.current = onEvent

  const connect = useCallback(() => {
    if (!threadId || doneRef.current) return
    if (wsRef.current && wsRef.current.readyState < 2) return // already open/connecting

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/${threadId}`)
    wsRef.current = ws

    ws.onopen = () => {
      setConnected(true)
      setLogs(p => p.length === 0 ? [{ level: 'system', message: 'pipeline connected', ts: new Date().toISOString() }] : p)
    }

    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data)
      if (msg.type === 'heartbeat') return
      if (msg.type === 'log') {
        setLogs(p => [...p, { level: msg.level, message: msg.message, ts: msg.timestamp }])
      } else {
        const label = msg.type === 'completed' ? 'done' : msg.type === 'error' ? 'error' : 'pause'
        const text  = msg.type === 'completed' ? 'pipeline step complete' :
                      msg.type === 'error'     ? msg.data?.error :
                                                 'awaiting confirmation'
        setLogs(p => [...p, { level: label, message: text, ts: msg.timestamp }])
        if (onEventRef.current) onEventRef.current(msg)
        if (msg.type === 'completed' || msg.type === 'error') {
          doneRef.current = true
        }
      }
    }

    ws.onerror = () => {
      setLogs(p => [...p, { level: 'error', message: 'connection error', ts: new Date().toISOString() }])
    }

    ws.onclose = () => {
      setConnected(false)
      // Auto-reconnect unless pipeline is done
      if (!doneRef.current) {
        setTimeout(connect, 2000)
      }
    }
  }, [threadId])

  useEffect(() => {
    doneRef.current = false
    setLogs([])
    connect()
    return () => {
      doneRef.current = true
      wsRef.current?.close()
    }
  }, [threadId, connect])

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }) }, [logs])

  const colors = {
    info: 'text-blue-400', warning: 'text-amber-400', error: 'text-red-400',
    success: 'text-emerald-400', debug: 'text-gray-600',
    system: 'text-gray-600', done: 'text-emerald-400', pause: 'text-amber-400',
  }
  const prefixes = {
    info: 'INF', warning: 'WRN', error: 'ERR', success: 'OK ',
    debug: 'DBG', system: 'SYS', done: 'DONE', pause: 'WAIT',
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <div className="flex items-center gap-2.5">
          <div className="relative w-2 h-2">
            {connected && <span className="absolute inset-0 rounded-full bg-emerald-400 ping-slow" />}
            <span className={`relative block w-2 h-2 rounded-full ${connected ? 'bg-emerald-400' : 'bg-gray-700'}`} />
          </div>
          <span className="text-xs font-mono text-gray-500 tracking-wider uppercase">
            {connected ? 'live' : 'reconnecting...'} · pipeline log
          </span>
        </div>
        <button onClick={() => setLogs([])} className="text-[10px] font-mono text-gray-700 hover:text-gray-400 tracking-wider uppercase">
          clear
        </button>
      </div>

      <div className="h-56 overflow-y-auto p-4 space-y-0.5 font-mono text-xs bg-black/20">
        {logs.length === 0 && (
          <span className="text-gray-700 cursor">waiting</span>
        )}
        {logs.map((log, i) => (
          <div key={i} className="flex items-baseline gap-3">
            <span className="text-gray-700 tabular-nums shrink-0">
              {new Date(log.ts).toLocaleTimeString('en-US', { hour12: false })}
            </span>
            <span className={`shrink-0 w-8 ${colors[log.level] || 'text-gray-500'} opacity-60`}>
              {prefixes[log.level] || '---'}
            </span>
            <span className={colors[log.level] || 'text-gray-400'}>{log.message}</span>
          </div>
        ))}
        <div ref={endRef} />
      </div>
    </div>
  )
}
