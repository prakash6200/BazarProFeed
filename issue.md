You are a senior Go backend engineer and distributed systems expert.

IMPORTANT:
You MUST analyze, identify issues, and MODIFY the code with fixes.

---

PHASE 1: UNDERSTAND THE CODEBASE

* Scan the entire repository
* Understand architecture and data flow:

  * How market feed (~2500 symbols) is received
  * How feed is processed
  * How WebSocket broadcasting works
  * How Redis caching is used
  * How PostgreSQL batch inserts are handled
* Identify concurrency model:

  * goroutines
  * channels
  * worker pools (if any)

Explain your understanding briefly.

---

PHASE 2: UNDERSTAND THE PROBLEM

Problem description:

* The Go Fiber server runs fine initially
* After some time:

  * Server becomes unreachable
  * Port is still in use (process is alive)
  * No API or WebSocket response
  * Nginx starts showing its default fallback page
* Memory usage remains stable (~700MB / 4GB)
* This indicates blocking, deadlock, or resource exhaustion

Important hint:
Server is not crashing — it is getting stuck and not accepting/responding to requests.

---

PHASE 3: ROOT CAUSE ANALYSIS

Find the exact cause in code by checking:

1. WebSocket:

   * Direct blocking writes (conn.WriteMessage)
   * Missing write pump / read pump
   * Slow client blocking broadcast loop

2. Concurrency:

   * Unbounded goroutines
   * Goroutine leaks
   * Channel blocking (full buffer, no consumer)

3. Deadlocks:

   * Mutex misuse
   * Blocking select without default

4. Database:

   * Blocking inserts in main flow
   * No context timeout
   * Connection pool exhaustion

5. Redis:

   * Blocking calls without timeout

6. Resource exhaustion:

   * File descriptors
   * Too many open connections
   * WebSocket connections not cleaned up

7. Fiber / fasthttp:

   * Blocking logic inside handlers
   * Missing timeouts

---

PHASE 4: FIX THE CODE (MANDATORY)

You MUST:

* Modify the existing code directly
* Replace blocking patterns with non-blocking ones
* Introduce:

  * WebSocket write pump (channel-based)
  * Worker pool for feed processing
  * Async DB queue for batch inserts
  * Context timeouts for DB/Redis
  * Safe channel usage
* Add proper connection cleanup for WebSockets
* Add limits for goroutines / connections

Provide updated code snippets for each fix.

---

PHASE 5: EXPLAIN WHY IT WAS FAILING

Explain clearly:

* Why the server becomes unreachable while port is still open
* Why Nginx fallback page appears
* What exact part of code caused blocking

---

PHASE 6: STRESS SCENARIO

Simulate and explain:

* Slow WebSocket client
* DB delay
* Channel buffer full

Show how old code fails and new code survives.

---

OUTPUT FORMAT:

1. System understanding
2. Root cause (exact)
3. Code fixes (modified code)
4. Explanation of failure
5. Improved architecture

Be strict. This is a high-load production system.

---

Here is my codebase:
