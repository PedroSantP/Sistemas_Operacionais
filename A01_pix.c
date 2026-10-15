/*
 * INF0336 - Primeira Avaliacao (A01) - Emulando o Sistema de Funcionamento do PIX
 * Discentes:
 * Pedro Henrique dos Santos Pereira - 202301481
 * Eduardo Civitate Peuvrie Sigilião - 202301433
 * Gustavo Sila Aguiar - 202306972
 * Compilar:
 *   gcc -Wall -O0 -pthread A01_pix.c -o A01_pix
 *
 * Uso:
 *   ./A01_pix corrida [N] [C]      (Q1) N requisicoes simultaneas SEM protecao (uma rodada)
 *   ./A01_pix experimento [R] [C]  (Q1) varre N e mede em quantas de R rodadas o erro aparece
 *                                  C = custo extra da validacao (alarga a janela; padrao 0)
 *   ./A01_pix fcfs [N] [P]         (Q2+Q3) pool de P workers + fila FCFS + debug por sinal
 *                                  sem N: usa o caso de exemplo fixo (8 requisicoes, P = 3)
 *                                  Ctrl+C  (SIGINT)  -> mostra o estado da fila
 *                                  kill -USR1 <pid>  -> idem
 *                                  Ctrl+\  (SIGQUIT) -> encerra o programa
 *
 * ---------------------------------------------------------------------------------------
 * Q1 (5.0p) - Sincronismo do PIX com thread pool e base de dados compartilhada
 * ---------------------------------------------------------------------------------------
 * Modelo N:1: N usuarios (contas 1..N) enviam, ao mesmo tempo, um PIX para UMA unica conta
 * de destino (conta 0, ex.: uma loja recebendo pagamentos). Cada requisicao e atendida por
 * uma thread da pool, e todas as threads acessam o mesmo vetor `contas[]` (a base de dados).
 *
 *  - RECURSO compartilhado: o saldo da conta de destino `contas[0].saldo` (a base de dados).
 *    Os saldos das contas de origem nao sao disputados, pois cada usuario envia um so PIX.
 *
 *  - SECAO CRITICA: o trecho "ler saldo -> validar -> escrever saldo" em atender_pix_q1().
 *    Ele nao e atomico: a thread le o saldo para uma variavel local, gasta um tempo
 *    validando (antifraude/limite) e so depois grava saldo_lido + valor.
 *
 *  - CONDICAO DE CORRIDA: se duas threads leem o mesmo saldo antes de qualquer uma gravar,
 *    a segunda escrita sobrescreve a primeira (lost update). O dinheiro sai da origem mas
 *    nao chega ao destino -> o total de dinheiro do sistema deixa de ser conservado.
 *    O resultado final depende da ordem em que o escalonador intercala as threads.
 *
 *  - CORRECAO: exclusao mutua com pthread_mutex em torno da secao critica (modo "com mutex"
 *    do experimento e na Q2), garantindo que so uma thread por vez altere a base.
 *
 * Para mostrar empiricamente a partir de que N o problema aparece, o modo "experimento"
 * executa R rodadas para cada N e conta quantas terminaram com saldo inconsistente.
 * Os valores exatos dependem da maquina (numero de nucleos, carga); a tabela impressa
 * pelo programa e a evidencia empirica a ser mostrada no video.
 *
 * Resultado medido (12 nucleos, 30 rodadas por N, custo 0 = nenhum atraso artificial):
 *      N:   1    2    4    8    16   32   64   128  256  512  1000
 *   erro:   0%   0%   0%   7%   13%  30%  67%  90%  70%  97%  97%
 *   -> o problema surge esporadicamente a partir de N ~ 8 e se torna evidente
 *      (maioria das rodadas erradas) a partir de N ~ 64. Com "custo" 100 (validacao mais
 *      demorada, janela maior) ja aparece com N = 4. Com mutex: 0% em todos os casos.
 *
 * ---------------------------------------------------------------------------------------
 * Q2 (3.0p) - Protecao minima + escalonador FCFS
 * ---------------------------------------------------------------------------------------
 * A pool passa a ter tamanho fixo P. As requisicoes chegam ao longo do tempo; se ha worker
 * livre, ela e atendida, senao entra numa fila FIFO (First-Come, First-Served). Workers
 * livres sempre retiram a requisicao da FRENTE da fila (a que chegou primeiro).
 *  - mtx_fila + cond_fila: protegem a fila e fazem os workers dormirem quando ela esta vazia
 *    (sem espera ocupada).
 *  - mtx_banco: protege a secao critica de acesso as contas (corrige a corrida da Q1).
 * Ao final e impressa a tabela de escalonamento (chegada, inicio, fim, espera) e uma
 * verificacao de que a ordem de inicio respeitou a ordem de chegada.
 *
 * ---------------------------------------------------------------------------------------
 * Q3 (2.0p) - Debugging por sinais
 * ---------------------------------------------------------------------------------------
 * SIGINT (Ctrl+C) e SIGUSR1 sao bloqueados em todas as threads e tratados por uma thread
 * dedicada que fica em sigwait(). Ao receber o sinal ela trava a fila e imprime o estado
 * atual: o que cada worker esta executando (e quanto falta), as requisicoes aguardando na
 * fila na ordem FCFS (com tempo de espera ate o momento) e quantas ja terminaram.
 * Usar uma thread com sigwait evita chamar printf/mutex dentro de um signal handler
 * (funcoes que nao sao async-signal-safe).
 * Os tempos de execucao (bursts) sao simulados com usleep, de 0.8 s a 2.5 s, para que haja
 * tempo de apertar a tecla e observar a fila mudando.
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define CONTA_DESTINO 0
#define SALDO_INICIAL 100000L /* R$ 1000,00 (valores em centavos) */
#define VALOR_PIX_Q1 1000L    /* R$ 10,00 */

typedef struct
{
    long saldo; /* centavos */
} Conta;

static Conta *contas;
static int num_contas;
static pthread_mutex_t mtx_banco = PTHREAD_MUTEX_INITIALIZER;

static struct timespec t0;

static long agora_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (t.tv_sec - t0.tv_sec) * 1000L + (t.tv_nsec - t0.tv_nsec) / 1000000L;
}

static void criar_contas(int n_usuarios)
{
    num_contas = n_usuarios + 1;
    contas = malloc(sizeof(Conta) * (size_t)num_contas);
    for (int i = 0; i < num_contas; i++)
        contas[i].saldo = SALDO_INICIAL;
}

static long total_do_sistema(void)
{
    long t = 0;
    for (int i = 0; i < num_contas; i++)
        t += contas[i].saldo;
    return t;
}

/* =====================================================================================
 *  Q1 - Condicao de corrida
 * ===================================================================================== */

typedef struct
{
    int origem;
    long valor;
} RequisicaoQ1;

static int usar_mutex_q1;
static int custo_validacao; /* iteracoes extras de "trabalho" dentro da secao critica */
static int largada;
static pthread_mutex_t mtx_largada = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond_largada = PTHREAD_COND_INITIALIZER;

/* Simula o processamento da transacao (consulta antifraude, limite diario, etc.) */
static void validar_transacao(void)
{
    for (volatile int i = 0; i < custo_validacao; i++)
        ;
}

static void *atender_pix_q1(void *arg)
{
    RequisicaoQ1 *r = arg;

    /* Todas as threads da pool esperam o sistema "abrir" para disparar juntas */
    pthread_mutex_lock(&mtx_largada);
    while (!largada)
        pthread_cond_wait(&cond_largada, &mtx_largada);
    pthread_mutex_unlock(&mtx_largada);

    if (usar_mutex_q1)
        pthread_mutex_lock(&mtx_banco);

    /* ================= SECAO CRITICA (acesso ao recurso compartilhado) ================= */
    long saldo_origem = contas[r->origem].saldo;
    long saldo_destino = contas[CONTA_DESTINO].saldo; /* leitura */
    validar_transacao();                              /* janela da condicao de corrida */
    if (saldo_origem >= r->valor)
    {
        contas[r->origem].saldo = saldo_origem - r->valor;
        contas[CONTA_DESTINO].saldo = saldo_destino + r->valor; /* escrita (pode sobrescrever) */
    }
    /* =================================================================================== */

    if (usar_mutex_q1)
        pthread_mutex_unlock(&mtx_banco);
    return NULL;
}

/* Executa uma rodada com N requisicoes simultaneas. Retorna o valor (centavos) perdido. */
static long rodada_q1(int n, int com_mutex)
{
    criar_contas(n);
    usar_mutex_q1 = com_mutex;
    largada = 0;

    pthread_t *pool = malloc(sizeof(pthread_t) * (size_t)n);
    RequisicaoQ1 *reqs = malloc(sizeof(RequisicaoQ1) * (size_t)n);
    long total_antes = total_do_sistema();

    for (int i = 0; i < n; i++)
    {
        reqs[i].origem = i + 1;
        reqs[i].valor = VALOR_PIX_Q1;
        pthread_create(&pool[i], NULL, atender_pix_q1, &reqs[i]);
    }

    pthread_mutex_lock(&mtx_largada);
    largada = 1;
    pthread_cond_broadcast(&cond_largada);
    pthread_mutex_unlock(&mtx_largada);

    for (int i = 0; i < n; i++)
        pthread_join(pool[i], NULL);

    long perdido = total_antes - total_do_sistema();
    free(pool);
    free(reqs);
    free(contas);
    return perdido;
}

static void modo_corrida(int n)
{
    printf("=== Q1: %d usuarios enviam R$ %.2f para a conta %d ao mesmo tempo (SEM mutex) ===\n",
           n, VALOR_PIX_Q1 / 100.0, CONTA_DESTINO);

    long esperado = SALDO_INICIAL + (long)n * VALOR_PIX_Q1;

    /* mesma logica de rodada_q1, mas mostrando o saldo do destino */
    criar_contas(n);
    usar_mutex_q1 = 0;
    largada = 0;
    pthread_t *pool = malloc(sizeof(pthread_t) * (size_t)n);
    RequisicaoQ1 *reqs = malloc(sizeof(RequisicaoQ1) * (size_t)n);
    for (int i = 0; i < n; i++)
    {
        reqs[i].origem = i + 1;
        reqs[i].valor = VALOR_PIX_Q1;
        pthread_create(&pool[i], NULL, atender_pix_q1, &reqs[i]);
    }
    pthread_mutex_lock(&mtx_largada);
    largada = 1;
    pthread_cond_broadcast(&cond_largada);
    pthread_mutex_unlock(&mtx_largada);
    for (int i = 0; i < n; i++)
        pthread_join(pool[i], NULL);

    long obtido = contas[CONTA_DESTINO].saldo;
    printf("Saldo esperado na conta destino: R$ %.2f\n", esperado / 100.0);
    printf("Saldo obtido   na conta destino: R$ %.2f\n", obtido / 100.0);
    if (obtido != esperado)
        printf(">> CONDICAO DE CORRIDA: R$ %.2f sumiram (%ld PIX perdidos)\n",
               (esperado - obtido) / 100.0, (esperado - obtido) / VALOR_PIX_Q1);
    else
        printf(">> Nesta rodada o resultado ficou correto (tente um N maior)\n");

    free(pool);
    free(reqs);
    free(contas);
}

static void modo_experimento(int rodadas)
{
    int ns[] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000};
    int qtd = (int)(sizeof(ns) / sizeof(ns[0]));

    printf("=== Q1: Experimento empirico (%d rodadas por N, PIX de R$ %.2f, custo %d) ===\n\n",
           rodadas, VALOR_PIX_Q1 / 100.0, custo_validacao);
    printf("%6s | %-26s | %-18s | %s\n", "N", "SEM mutex: rodadas c/ erro",
           "PIX perdidos (med)", "COM mutex: rodadas c/ erro");
    printf("-------+----------------------------+--------------------+---------------------------\n");

    for (int k = 0; k < qtd; k++)
    {
        int n = ns[k];
        int erros = 0, erros_mutex = 0;
        long perdidos = 0;
        for (int r = 0; r < rodadas; r++)
        {
            long p = rodada_q1(n, 0);
            if (p != 0)
            {
                erros++;
                perdidos += p / VALOR_PIX_Q1;
            }
            if (rodada_q1(n, 1) != 0)
                erros_mutex++;
        }
        printf("%6d | %5d/%-5d (%5.1f%%)       | %18.1f | %5d/%-5d\n", n, erros, rodadas,
               100.0 * erros / rodadas, (double)perdidos / rodadas, erros_mutex, rodadas);
    }
    printf("\nO N a partir do qual a coluna 'SEM mutex' deixa de ser 0 e onde o problema se\n"
           "torna evidente. Com mutex o erro nunca ocorre.\n");
}

/* =====================================================================================
 *  Q2 + Q3 - Thread pool de tamanho fixo + escalonador FCFS + debugging por sinais
 * ===================================================================================== */

typedef struct
{
    int id;
    int origem;
    long valor;
    int chegada_ms; /* instante de chegada (relativo ao inicio) */
    int burst_ms;   /* tempo de execucao simulado */
    long t_chegada, t_inicio, t_fim;
    int worker;
    int aprovado;
} RequisicaoPIX;

/* Fila FCFS circular (de ponteiros para as requisicoes) */
static RequisicaoPIX **fila;
static int fila_cap, fila_ini, fila_tam;

static pthread_mutex_t mtx_fila = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond_fila = PTHREAD_COND_INITIALIZER;
static int producao_encerrada;

static int pool_tam;
static RequisicaoPIX **executando; /* executando[w] = requisicao do worker w (ou NULL) */
static int total_reqs, concluidas, chegaram;

static void enfileirar(RequisicaoPIX *r)
{
    fila[(fila_ini + fila_tam) % fila_cap] = r;
    fila_tam++;
}

static RequisicaoPIX *desenfileirar(void)
{
    RequisicaoPIX *r = fila[fila_ini];
    fila_ini = (fila_ini + 1) % fila_cap;
    fila_tam--;
    return r;
}

static void *worker_fcfs(void *arg)
{
    int w = (int)(long)arg;

    for (;;)
    {
        pthread_mutex_lock(&mtx_fila);
        while (fila_tam == 0 && !producao_encerrada)
            pthread_cond_wait(&cond_fila, &mtx_fila);
        if (fila_tam == 0 && producao_encerrada)
        {
            pthread_mutex_unlock(&mtx_fila);
            break;
        }
        RequisicaoPIX *r = desenfileirar(); /* sempre a que chegou primeiro */
        r->t_inicio = agora_ms();
        r->worker = w;
        executando[w] = r;
        printf("[%6.3fs] W%d inicia PIX #%d (esperou %.3fs na fila) | fila: %d\n",
               r->t_inicio / 1000.0, w + 1, r->id, (r->t_inicio - r->t_chegada) / 1000.0,
               fila_tam);
        pthread_mutex_unlock(&mtx_fila);

        /* Tempo de execucao simulado (comunicacao com o BACEN, antifraude...) */
        usleep((useconds_t)r->burst_ms * 1000);

        /* Secao critica protegida: debito e credito atomicos */
        pthread_mutex_lock(&mtx_banco);
        if (contas[r->origem].saldo >= r->valor)
        {
            contas[r->origem].saldo -= r->valor;
            contas[CONTA_DESTINO].saldo += r->valor;
            r->aprovado = 1;
        }
        pthread_mutex_unlock(&mtx_banco);

        pthread_mutex_lock(&mtx_fila);
        r->t_fim = agora_ms();
        executando[w] = NULL;
        concluidas++;
        printf("[%6.3fs] W%d conclui PIX #%d (%s) | worker livre\n", r->t_fim / 1000.0, w + 1,
               r->id, r->aprovado ? "aprovado" : "recusado: saldo insuficiente");
        pthread_mutex_unlock(&mtx_fila);
    }
    return NULL;
}

/* Q3: imprime o estado atual da fila de processos */
static void mostrar_estado(int sinal)
{
    pthread_mutex_lock(&mtx_fila);
    long t = agora_ms();
    printf("\n================ DEBUG (%s) t = %.3fs ================\n",
           sinal == SIGINT ? "SIGINT" : "SIGUSR1", t / 1000.0);

    printf("Thread pool (%d workers):\n", pool_tam);
    for (int w = 0; w < pool_tam; w++)
    {
        RequisicaoPIX *r = executando[w];
        if (r)
        {
            long restante = r->t_inicio + r->burst_ms - t;
            printf("  W%d: EXECUTANDO PIX #%d (conta %d -> %d, R$ %.2f) | restante ~%.3fs\n",
                   w + 1, r->id, r->origem, CONTA_DESTINO, r->valor / 100.0,
                   (restante > 0 ? restante : 0) / 1000.0);
        }
        else
        {
            printf("  W%d: livre\n", w + 1);
        }
    }

    printf("Fila FCFS (%d aguardando) - frente primeiro:\n", fila_tam);
    if (fila_tam == 0)
        printf("  (vazia)\n");
    for (int i = 0; i < fila_tam; i++)
    {
        RequisicaoPIX *r = fila[(fila_ini + i) % fila_cap];
        printf("  %2d. PIX #%-3d conta %3d -> %d  R$ %8.2f  chegou %.3fs  esperando %.3fs  "
               "burst %.1fs\n",
               i + 1, r->id, r->origem, CONTA_DESTINO, r->valor / 100.0,
               r->t_chegada / 1000.0, (t - r->t_chegada) / 1000.0, r->burst_ms / 1000.0);
    }
    printf("Concluidas: %d/%d | Ainda nao chegaram: %d\n", concluidas, total_reqs,
           total_reqs - chegaram);
    pthread_mutex_lock(&mtx_banco);
    printf("Saldo atual da conta destino: R$ %.2f\n", contas[CONTA_DESTINO].saldo / 100.0);
    pthread_mutex_unlock(&mtx_banco);
    printf("=============================================================\n\n");
    pthread_mutex_unlock(&mtx_fila);
}

static sigset_t sinais_debug;

static void *thread_debug(void *arg)
{
    (void)arg;
    for (;;)
    {
        int sinal;
        if (sigwait(&sinais_debug, &sinal) == 0)
            mostrar_estado(sinal);
    }
    return NULL;
}

static int cmp_id(const void *a, const void *b)
{
    return ((const RequisicaoPIX *)a)->id - ((const RequisicaoPIX *)b)->id;
}

static int cmp_inicio(const void *a, const void *b)
{
    const RequisicaoPIX *x = *(RequisicaoPIX *const *)a, *y = *(RequisicaoPIX *const *)b;
    if (x->t_inicio != y->t_inicio)
        return x->t_inicio < y->t_inicio ? -1 : 1;
    return x->id - y->id;
}

static void modo_fcfs(int n, int p)
{
    RequisicaoPIX *reqs;

    if (n <= 0)
    {
        /* Caso de exemplo fixo (reprodutivel para o video) */
        static const int ex[][3] = {
            /* chegada_ms, burst_ms, valor (reais) */
            {0, 2000, 50},
            {200, 1500, 120},
            {400, 2500, 30},
            {600, 1000, 75},
            {800, 1800, 200},
            {1000, 1200, 15},
            {1200, 2200, 90},
            {1400, 800, 60},
        };
        n = (int)(sizeof(ex) / sizeof(ex[0]));
        p = 3;
        reqs = calloc((size_t)n, sizeof(RequisicaoPIX));
        for (int i = 0; i < n; i++)
        {
            reqs[i].chegada_ms = ex[i][0];
            reqs[i].burst_ms = ex[i][1];
            reqs[i].valor = ex[i][2] * 100L;
        }
    }
    else
    {
        srand(42);
        reqs = calloc((size_t)n, sizeof(RequisicaoPIX));
        int t = 0;
        for (int i = 0; i < n; i++)
        {
            reqs[i].chegada_ms = t;
            reqs[i].burst_ms = 800 + rand() % 1701;     /* 0.8s a 2.5s */
            reqs[i].valor = (10 + rand() % 491) * 100L; /* R$ 10 a R$ 500 */
            t += 100 + rand() % 301;                    /* chegadas a cada 0.1-0.4s */
        }
    }
    for (int i = 0; i < n; i++)
    {
        reqs[i].id = i + 1;
        reqs[i].origem = i + 1;
        reqs[i].worker = -1;
    }

    criar_contas(n);
    total_reqs = n;
    pool_tam = p;
    fila_cap = n;
    fila = malloc(sizeof(RequisicaoPIX *) * (size_t)n);
    executando = calloc((size_t)p, sizeof(RequisicaoPIX *));
    long total_antes = total_do_sistema();

    /* Q3: bloqueia SIGINT/SIGUSR1 antes de criar threads (todas herdam a mascara) */
    sigemptyset(&sinais_debug);
    sigaddset(&sinais_debug, SIGINT);
    sigaddset(&sinais_debug, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &sinais_debug, NULL);

    printf("=== Q2/Q3: PIX com thread pool de %d workers + fila FCFS (PID %d) ===\n", p,
           (int)getpid());
    printf("Ctrl+C ou 'kill -USR1 %d' mostra a fila | Ctrl+\\ encerra\n\n", (int)getpid());
    printf("Requisicoes previstas:\n  %4s %8s %7s %10s\n", "PIX", "chegada", "burst", "valor");
    for (int i = 0; i < n; i++)
        printf("  #%-3d %7.1fs %6.1fs %10.2f\n", reqs[i].id, reqs[i].chegada_ms / 1000.0,
               reqs[i].burst_ms / 1000.0, reqs[i].valor / 100.0);
    printf("\n");

    pthread_t dbg;
    pthread_t *pool = malloc(sizeof(pthread_t) * (size_t)p);
    clock_gettime(CLOCK_MONOTONIC, &t0);
    pthread_create(&dbg, NULL, thread_debug, NULL);
    for (int w = 0; w < p; w++)
        pthread_create(&pool[w], NULL, worker_fcfs, (void *)(long)w);

    /* Gerador de requisicoes: cada uma chega no seu instante e entra no fim da fila */
    for (int i = 0; i < n; i++)
    {
        long falta = reqs[i].chegada_ms - agora_ms();
        if (falta > 0)
            usleep((useconds_t)falta * 1000);

        pthread_mutex_lock(&mtx_fila);
        reqs[i].t_chegada = agora_ms();
        chegaram++;
        int ocupados = 0;
        for (int w = 0; w < p; w++)
            ocupados += executando[w] != NULL;
        enfileirar(&reqs[i]);
        if (ocupados + fila_tam > p)
            printf("[%6.3fs] Chegou PIX #%d -> pool cheia, aguarda na fila FCFS (posicao %d)\n",
                   reqs[i].t_chegada / 1000.0, reqs[i].id, fila_tam);
        else
            printf("[%6.3fs] Chegou PIX #%d -> ha worker livre\n", reqs[i].t_chegada / 1000.0,
                   reqs[i].id);
        pthread_cond_signal(&cond_fila);
        pthread_mutex_unlock(&mtx_fila);
    }

    pthread_mutex_lock(&mtx_fila);
    producao_encerrada = 1;
    pthread_cond_broadcast(&cond_fila);
    pthread_mutex_unlock(&mtx_fila);

    for (int w = 0; w < p; w++)
        pthread_join(pool[w], NULL);
    pthread_cancel(dbg); /* sigwait e ponto de cancelamento */
    pthread_join(dbg, NULL);

    /* Relatorio do escalonamento FCFS */
    qsort(reqs, (size_t)n, sizeof(RequisicaoPIX), cmp_id);
    printf("\n=== Tabela de escalonamento FCFS ===\n");
    printf("%5s | %8s | %6s | %8s | %8s | %7s | %s\n", "PIX", "chegada", "burst", "inicio",
           "fim", "espera", "worker");
    printf("------+----------+--------+----------+----------+---------+-------\n");
    double soma_espera = 0;
    for (int i = 0; i < n; i++)
    {
        RequisicaoPIX *r = &reqs[i];
        double espera = (r->t_inicio - r->t_chegada) / 1000.0;
        soma_espera += espera;
        printf("  #%-3d| %7.3fs | %5.1fs | %7.3fs | %7.3fs | %6.3fs | W%d\n", r->id,
               r->t_chegada / 1000.0, r->burst_ms / 1000.0, r->t_inicio / 1000.0,
               r->t_fim / 1000.0, espera, r->worker + 1);
    }
    printf("Espera media: %.3fs\n", soma_espera / n);

    RequisicaoPIX **ordem = malloc(sizeof(RequisicaoPIX *) * (size_t)n);
    for (int i = 0; i < n; i++)
        ordem[i] = &reqs[i];
    qsort(ordem, (size_t)n, sizeof(RequisicaoPIX *), cmp_inicio);
    int fcfs_ok = 1;
    printf("Ordem de inicio:");
    for (int i = 0; i < n; i++)
    {
        printf(" #%d", ordem[i]->id);
        if (ordem[i]->id != i + 1)
            fcfs_ok = 0;
    }
    printf("\n>> %s\n", fcfs_ok ? "Ordem de atendimento = ordem de chegada (FCFS respeitado)"
                                : "Ordem de atendimento diferente da ordem de chegada!");

    printf("Saldo final da conta destino: R$ %.2f | dinheiro total %s\n",
           contas[CONTA_DESTINO].saldo / 100.0,
           total_do_sistema() == total_antes ? "conservado (sem condicao de corrida)"
                                             : "INCONSISTENTE");

    free(ordem);
    free(pool);
    free(fila);
    free(executando);
    free(reqs);
    free(contas);
}

int main(int argc, char *argv[])
{
    const char *modo = argc > 1 ? argv[1] : "";

    if (strcmp(modo, "corrida") == 0)
    {
        custo_validacao = argc > 3 ? atoi(argv[3]) : 0;
        modo_corrida(argc > 2 ? atoi(argv[2]) : 1000);
    }
    else if (strcmp(modo, "experimento") == 0)
    {
        custo_validacao = argc > 3 ? atoi(argv[3]) : 0;
        modo_experimento(argc > 2 ? atoi(argv[2]) : 50);
    }
    else if (strcmp(modo, "fcfs") == 0)
    {
        int n = argc > 2 ? atoi(argv[2]) : 0;
        int p = argc > 3 ? atoi(argv[3]) : 3;
        if (n > 0 && p <= 0)
        {
            fprintf(stderr, "P deve ser > 0\n");
            return 1;
        }
        modo_fcfs(n, p);
    }
    else
    {
        fprintf(stderr,
                "Uso:\n"
                "  %s corrida [N] [C]    (Q1) uma rodada com N PIX simultaneos sem mutex\n"
                "  %s experimento [R] [C](Q1) varre N com R rodadas cada\n"
                "  %s fcfs [N] [P]       (Q2/Q3) pool de P workers + fila FCFS + debug\n",
                argv[0], argv[0], argv[0]);
        return 1;
    }
    return 0;
}
