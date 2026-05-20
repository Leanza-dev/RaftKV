# Engineering at Scale: RaftKV Architecture

Bem-vindo ao *Architecture Decision Record* (ADR) do RaftKV. Este documento expõe as decisões de design que nos permitem atingir latências de eleição na casa dos milissegundos e resiliência a partições de rede, focando em segurança de memória e concorrência extrema.

## 1. Concorrência Dinâmica com `JoinSet`
Em sistemas distribuídos, o *Head-of-Line Blocking* é letal. A latência de um nó nunca deve definir o *throughput* do cluster.
- **A Solução:** Utilizamos o `tokio::task::JoinSet` para paralelizar as chamadas RPC (`RequestVote` e `AppendEntries`). Isso permite processar as respostas na ordem exata de chegada (`join_next().await`).
- **Fail-Fast Gracioso:** No instante em que atingimos o Quórum de votos (maioria simples), nós assumimos a liderança instantaneamente e disparamos `set.abort_all()`.
- **Prevenção de Resource Leaks:** Ao invés de vazar conexões, o abortamento das tasks no Tokio aciona o `Drop` da Future, que realiza um *graceful teardown* no nível do socket (ex: enviando um pacote FIN), devolvendo os recursos ao Sistema Operacional imediatamente.

## 2. Mitigando Saturação de Rede (Microbursting)
Com o uso de fan-out agressivo, enviar centenas de chamadas RPC simultaneamente para os *Learners* ou *Followers* criaria um pico agudo de tráfego (Microburst), estourando os buffers dos *switches* de rede.
- **Defesa Ativa (Implementada):** Injetamos um `tokio::sync::Semaphore` na raiz do `JoinSet`. Ao adquirir *permits* (`acquire_owned`), limitamos a vazão concorrente a lotes seguros (ex: *Batches* de 50). Isso estabiliza a latência P99 e protege a infraestrutura de rede.

## 3. Isolamento de CPU e Starvation
O Raft exige escrita persistente num Log Seguro (Write-Ahead Log) no disco. Operações de *fsync* em disco são bloqueantes no nível do Kernel. Se executadas diretamente, causariam *Thread Starvation* no *worker* do Tokio.
- **Defesa Ativa:** Todo I/O bloqueante (ou simulação futura) é isolado utilizando `tokio::task::spawn_blocking`. Isso despacha a carga para uma *Thread Pool* especializada, garantindo que o *loop* principal de eleição e *heartbeats* rode de forma 100% assíncrona, não preemptiva e imune a travamentos.
