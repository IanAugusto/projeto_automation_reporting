# Projeto de Automação de Relatórios

Este projeto implementa um pipeline completo de análise de dados usando **Pandas** para processamento eficiente de datasets de vendas de café.

## 📁 Estrutura do Projeto

```
projeto_automation_reporting/
│
├── data/                    # Dados do projeto
│   ├── raw/                # Dados originais (CSV de vendas)
│   └── processed/          # Dados processados (com anomalias detectadas)
├── reports/                 # Relatórios gerados
│
├── pandas_version/          # Scripts principais do projeto
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


## 📊 Funcionalidades

- **Ideal para**: Datasets de vendas de café, análises rápidas e eficientes
- **Características**: 
  - Processamento em memória com Pandas
  - Interface simples e intuitiva
  - Detecção automática de anomalias
  - Geração de relatórios visuais
  - Automação completa do pipeline

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

### Execução Rápida

```bash
# Executa o pipeline completo uma vez
cd pandas_version
python automation.py --mode once

# Executa em modo agendado (diário às 9h)
python automation.py --mode schedule
```

### Execução Individual dos Módulos

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

## ⚙️ Configuração

### Arquivo de Configuração (JSON)
```json
{
    "data_path": "data/raw/",
    "processed_path": "data/processed/",
    "reports_path": "reports/",
    "input_file": "Coffe_sales.csv",
    "output_file": "Coffe_sales_with_anomalies.csv",
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

O projeto inclui geração automática de dados de exemplo de vendas de café com:
- **Vendas**: Hora, tipo de pagamento, valor, nome do café, período do dia
- **Período**: 1000+ transações distribuídas ao longo do tempo
- **Produtos**: 5 tipos diferentes de café (Latte, Americano, Hot Chocolate, Cappuccino, Espresso)
- **Padrões**: Variações sazonais, horários de pico e anomalias

## 📊 Relatórios Gerados

### PDF
- Resumo executivo de vendas de café
- Gráficos de vendas temporais
- Top produtos de café
- Distribuição de vendas por período
- Análise de categorias de café

### Excel
- Dados originais de vendas
- Resumo de métricas de vendas
- Top produtos de café
- Vendas por período do dia
- Estatísticas por tipo de café

### Imagens
- Gráfico de vendas de café ao longo do tempo
- Top produtos de café
- Distribuição de valores de venda
- Heatmap produto x período do dia
- Gráfico de distribuição de vendas

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
- `automation.log` - Logs do pipeline principal

### Informações Registradas
- Início e fim de cada etapa
- Contagem de registros processados
- Erros e exceções
- Tempo de execução
- Configurações utilizadas

## 🛠️ Desenvolvimento

### Estrutura de Classes

- `DataExtractor`: Extração de dados de vendas de café
- `DataTransformer`: Transformação e limpeza de dados
- `AnomalyDetector`: Detecção de anomalias nas vendas
- `ReportGenerator`: Geração de relatórios visuais
- `DataPipeline`: Orquestração completa do pipeline

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

- [ ] Suporte a mais formatos de dados
- [ ] Alertas por email/Slack
- [ ] Dashboard em tempo real
- [ ] Machine Learning avançado
- [ ] Suporte a streaming de dados

---

**Desenvolvido com ❤️ para automação de relatórios de dados**
