import { useEffect, useState } from 'react'
import type { QueryClient } from '@tanstack/react-query'

export type StreamState = 'connecting' | 'live' | 'closed'

/** Topics the dashboard subscribes to. Log topics are opened per-run in M3. */
const TOPICS = ['incidents', 'runs', 'budget'] as const

interface BusEvent {
  id: number
  topic: string
  type: string
  data?: unknown
}

/**
 * Opens one EventSource multiplexed across every topic and keeps the query
 * cache fresh from it.
 *
 * A `resync` event means the server dropped this client's buffer because it
 * fell behind (see internal/bus/bus.go). The only correct response is to
 * invalidate and refetch over HTTP — the missed events are gone, and trying to
 * reconstruct state from the stream would leave the UI quietly wrong. That is
 * the self-healing path SPEC §9 specifies, and it is why there is no second
 * client-side state store.
 */
export function useSentinelStream(queryClient: QueryClient): StreamState {
  const [state, setState] = useState<StreamState>('connecting')

  useEffect(() => {
    const source = new EventSource(`/api/stream?topics=${TOPICS.join(',')}`)

    source.onopen = () => setState('live')

    source.onerror = () => {
      setState('closed')
      // EventSource reconnects on its own, replaying from Last-Event-ID. A
      // reconnect may have missed transitions, so refetch when it lands.
      void queryClient.invalidateQueries()
    }

    source.onmessage = (message: MessageEvent<string>) => {
      setState('live')

      let event: BusEvent
      try {
        event = JSON.parse(message.data) as BusEvent
      } catch {
        return // a malformed frame must not tear down the stream
      }

      if (event.type === 'resync') {
        void queryClient.invalidateQueries()
        return
      }

      if (event.topic === 'incidents') {
        void queryClient.invalidateQueries({ queryKey: ['incidents'] })
        void queryClient.invalidateQueries({ queryKey: ['overview'] })
      }
    }

    return () => {
      source.close()
      setState('closed')
    }
  }, [queryClient])

  return state
}
