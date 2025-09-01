# Projeto de Automação de Relatórios

Este projeto implementa um pipeline completo de análise de dados com duas versões: uma usando **Pandas** para datasets menores e outra usando **Apache Spark** para processamento de big data.

## 📁 Estrutura do Projeto

```
projeto_automation_reporting/
│
├── data/                    # Dados brutos (CSV de vendas, etc.)
├── reports/                 # Relatórios gerados
│
├── pandas_version/          # Scripts usando Pandas
│   ├── extract.py          # Extração de dados
│   ├── transform.py        # Transformação e limpeza
│   ├── anomaly.py          # Detecção de anomalias
│   ├── report.py           # Geração de relatórios
│   └── automation.py       # Automação do pipeline
│
├── spark_version/           # Scripts usando PySpark
│   ├── extract.py          # Extração de dados
│   ├── transform.py        # Transformação e limpeza
│   ├── anomaly.py          # Detecção de anomalias
│   ├── report.py           # Geração de relatórios
│   └── automation.py       # Automação do pipeline
│
├── requirements.txt         # Dependências do projeto
└── README.md               # Este arquivo
```

## 🚀 Instalação

### 1. Clone o repositório
```bash
git clone <url-do-repositorio>
cd projeto_automation_reporting
```

### 2. Crie um ambiente virtual
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

### 3. Instale as dependências
```bash
pip install -r requirements.txt
```

### 4. Instale o Apache Spark (apenas para versão Spark)
```bash
# Baixe o Spark do site oficial: https://spark.apache.org/downloads.html
# Configure as variáveis de ambiente SPARK_HOME e PATH
```

## 📊 Funcionalidades

### Versão Pandas
- **Ideal para**: Datasets até 1GB, análises rápidas
- **Características**: 
  - Processamento em memória
  - Interface simples e intuitiva
  - Ideal para prototipagem

### Versão Spark
- **Ideal para**: Big data, datasets > 1GB
- **Características**:
  - Processamento distribuído
  - Escalabilidade horizontal
  - Otimizações automáticas

## 🔧 Módulos

### 1. Extract (Extração)
- Extração de dados de CSV, Parquet, bancos de dados
- Validação de dados
- Criação de dados de exemplo

### 2. Transform (Transformação)
- Limpeza de dados (duplicatas, valores ausentes)
- Criação de colunas calculadas
- Agregações e pivots
- Detecção e remoção de outliers

### 3. Anomaly (Detecção de Anomalias)
- **Métodos estatísticos**: IQR, Z-Score
- **Machine Learning**: Isolation Forest, K-Means
- **Temporal**: Detecção de padrões anômalos no tempo
- **Relatórios**: Análise detalhada de anomalias

### 4. Report (Relatórios)
- **Visualizações**: Gráficos de linha, barras, heatmaps
- **Formatos**: PDF, Excel, imagens PNG
- **Métricas**: KPIs, estatísticas descritivas
- **Dashboards**: Resumos executivos

### 5. Automation (Automação)
- **Execução única**: Pipeline completo em uma execução
- **Agendamento**: Execução diária automática
- **Configuração**: Arquivos JSON para personalização
- **Logs**: Rastreamento completo de execuções

## 🎯 Como Usar

### Execução Rápida - Versão Pandas

```bash
# Executa o pipeline completo uma vez
cd pandas_version
python automation.py --mode once

# Executa em modo agendado (diário às 9h)
python automation.py --mode schedule
```

### Execução Rápida - Versão Spark

```bash
# Executa o pipeline completo uma vez
cd spark_version
python automation.py --mode once

# Executa em modo agendado (diário às 9h)
python automation.py --mode schedule
```

### Execução Individual dos Módulos

#### Pandas
```bash
cd pandas_version

# 1. Extração
python extract.py

# 2. Transformação
python transform.py

# 3. Detecção de anomalias
python anomaly.py

# 4. Geração de relatórios
python report.py
```

#### Spark
```bash
cd spark_version

# 1. Extração
python extract.py

# 2. Transformação
python transform.py

# 3. Detecção de anomalias
python anomaly.py

# 4. Geração de relatórios
python report.py
```

## ⚙️ Configuração

### Arquivo de Configuração (JSON)
```json
{
    "data_path": "data/",
    "reports_path": "reports/",
    "input_file": "sales_data.csv",
    "output_file": "processed_sales_data.csv",
    "anomaly_threshold": 0.1,
    "enable_anomaly_detection": true,
    "enable_reporting": true,
    "schedule_time": "09:00",
    "retention_days": 30
}
```

### Usando Configuração Personalizada
```bash
python automation.py --mode once --config config.json
```

## 📈 Exemplo de Dados

O projeto inclui geração automática de dados de exemplo com:
- **Vendas**: ID, data, produto, quantidade, preço, total
- **Período**: 1000 transações distribuídas ao longo do tempo
- **Produtos**: 5 produtos diferentes
- **Padrões**: Variações sazonais e anomalias

## 📊 Relatórios Gerados

### PDF
- Resumo executivo
- Gráficos de vendas temporais
- Top produtos
- Distribuição de vendas
- Análise de categorias

### Excel
- Dados originais
- Resumo de métricas
- Top produtos
- Vendas mensais
- Estatísticas por categoria

### Imagens
- Gráfico de vendas ao longo do tempo
- Top 10 produtos
- Distribuição de valores
- Heatmap produto x mês
- Gráfico de pizza por categoria

## 🔍 Detecção de Anomalias

### Métodos Implementados

1. **IQR (Interquartile Range)**
   - Detecta outliers baseado em quartis
   - Threshold configurável (padrão: 1.5)

2. **Z-Score**
   - Detecta valores que se desviam significativamente da média
   - Threshold configurável (padrão: 3)

3. **Isolation Forest**
   - Algoritmo de ML para detecção de anomalias
   - Contaminação configurável (padrão: 10%)

4. **K-Means Clustering**
   - Agrupa dados e identifica pontos distantes dos centróides
   - Número de clusters configurável

5. **Análise Temporal**
   - Detecta padrões anômalos ao longo do tempo
   - Baseado em média móvel e desvio padrão

## 🚨 Logs e Monitoramento

### Arquivos de Log
- `automation.log` - Logs da versão Pandas
- `spark_automation.log` - Logs da versão Spark

### Informações Registradas
- Início e fim de cada etapa
- Contagem de registros processados
- Erros e exceções
- Tempo de execução
- Configurações utilizadas

## 🛠️ Desenvolvimento

### Estrutura de Classes

#### Pandas
- `DataExtractor`: Extração de dados
- `DataTransformer`: Transformação de dados
- `AnomalyDetector`: Detecção de anomalias
- `ReportGenerator`: Geração de relatórios
- `DataPipeline`: Orquestração do pipeline

#### Spark
- `SparkDataExtractor`: Extração com Spark
- `SparkDataTransformer`: Transformação com Spark
- `SparkAnomalyDetector`: Detecção com Spark
- `SparkReportGenerator`: Relatórios com Spark
- `SparkDataPipeline`: Pipeline Spark

### Extensibilidade

O projeto foi projetado para ser facilmente extensível:

1. **Novos formatos de dados**: Adicione métodos em `extract.py`
2. **Novas transformações**: Estenda `transform.py`
3. **Novos algoritmos de anomalia**: Implemente em `anomaly.py`
4. **Novos tipos de relatório**: Adicione em `report.py`

## 📋 Requisitos do Sistema

### Mínimos
- Python 3.8+
- 4GB RAM
- 1GB espaço em disco

### Recomendados
- Python 3.10+
- 8GB RAM
- 5GB espaço em disco
- Apache Spark 3.4+ (para versão Spark)

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 🆘 Suporte

Para dúvidas ou problemas:

1. Verifique os logs de execução
2. Consulte a documentação dos módulos
3. Abra uma issue no repositório
4. Entre em contato com a equipe de desenvolvimento

## 🔄 Atualizações Futuras

- [ ] Interface web para configuração
- [ ] Suporte a mais formatos de dados
- [ ] Integração com bancos de dados NoSQL
- [ ] Alertas por email/Slack
- [ ] Dashboard em tempo real
- [ ] Machine Learning avançado
- [ ] Suporte a streaming de dados

---

**Desenvolvido com ❤️ para automação de relatórios de dados**
