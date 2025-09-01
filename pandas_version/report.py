"""
Módulo de Geração de Relatórios - Versão Pandas
Responsável por criar relatórios visuais e em PDF dos dados analisados
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do matplotlib para português
plt.rcParams['font.size'] = 10
plt.rcParams['figure.figsize'] = (12, 8)

class EmailConfig:
    """Classe para configuração de email"""
    
    def __init__(self):
        """Inicializa configurações de email - CONFIGURE AQUI SUAS CREDENCIAIS"""
        # ===== CONFIGURAÇÕES GMAIL - PREENCHA COM SEUS DADOS =====
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = "ian.prog.br@gmail.com"  # ← SEU EMAIL GMAIL AQUI
        self.sender_password = "cvei cvyf pqtq wiov"  # ← SUA APP PASSWORD AQUI
        self.recipient_email = [ "mc.at@hotmail.com" ]  # ← EMAIL DESTINATÁRIO AQUI
        
        # ===== CONFIGURAÇÕES OPCIONAIS =====
        self.email_subject = "Relatório de Vendas de Café - {date}"
        self.email_body = """
        Olá,
        
        Segue em anexo o relatório de vendas de café gerado automaticamente.
        
        O relatório inclui:
        - Análise de vendas por tipo de café
        - Gráficos de tendências
        - Relatório em PDF e Excel
        
        Data de geração: {date}
        
        Atenciosamente,
        Sistema de Relatórios Automatizados
        """
    
    def is_configured(self) -> bool:
        """Verifica se as configurações de email estão preenchidas"""
        return all([
            self.sender_email,
            self.sender_password,
            self.recipient_email
        ])
    
    def get_subject(self) -> str:
        """Retorna o assunto do email formatado"""
        return self.email_subject.format(date=datetime.now().strftime("%d/%m/%Y"))
    
    def get_body(self) -> str:
        """Retorna o corpo do email formatado"""
        return self.email_body.format(date=datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

class ReportGenerator:
    """Classe para geração de relatórios usando pandas e matplotlib"""
    
    def __init__(self, output_dir: str = "reports/"):
        """
        Inicializa o gerador de relatórios
        
        Args:
            output_dir (str): Diretório para salvar relatórios
        """
        self.output_dir = output_dir
        self.ensure_output_directory()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.email_config = EmailConfig()
    
    def ensure_output_directory(self):
        """Garante que o diretório de saída existe"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Diretório de relatórios criado: {self.output_dir}")
    
    def create_sales_summary_report(self, df: pd.DataFrame) -> Dict:
        """
        Cria relatório resumo de vendas
        
        Args:
            df (pd.DataFrame): DataFrame com dados de vendas
            
        Returns:
            Dict: Dicionário com métricas do relatório
        """
        report = {}
        
        # Métricas básicas
        report['total_sales'] = df['money'].sum() if 'money' in df.columns else 0
        report['total_transactions'] = len(df)
        report['average_transaction'] = df['money'].mean() if 'money' in df.columns else 0
        report['unique_products'] = df['coffee_name'].nunique() if 'coffee_name' in df.columns else 0
        
        # Métricas por período
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            report['date_range'] = {
                'start': df['Date'].min().strftime('%Y-%m-%d'),
                'end': df['Date'].max().strftime('%Y-%m-%d')
            }
            
            # Vendas por mês
            monthly_sales = df.groupby(df['Date'].dt.to_period('M'))['money'].sum()
            report['monthly_sales'] = monthly_sales.to_dict()
        
        # Top produtos
        if 'coffee_name' in df.columns and 'money' in df.columns:
            top_products = df.groupby('coffee_name')['money'].sum().sort_values(ascending=False)
            report['top_products'] = top_products.head(10).to_dict()
        
        logger.info("Relatório resumo de vendas criado")
        return report
    
    def create_visualizations(self, df: pd.DataFrame, report_data: Dict) -> List[str]:
        """
        Cria visualizações dos dados
        
        Args:
            df (pd.DataFrame): DataFrame com dados
            report_data (Dict): Dados do relatório
            
        Returns:
            List[str]: Lista de caminhos dos arquivos de imagem criados
        """
        image_paths = []
        
        # 1. Gráfico de vendas ao longo do tempo
        if 'Date' in df.columns:
            plt.figure(figsize=(12, 6))
            daily_sales = df.groupby(pd.to_datetime(df['Date']).dt.date)['money'].sum()
            plt.plot(daily_sales.index, daily_sales.values, linewidth=2)
            plt.title('Vendas Diárias de Café', fontsize=14, fontweight='bold')
            plt.xlabel('Data')
            plt.ylabel('Total de Vendas (R$)')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            time_plot_path = os.path.join(self.output_dir, f'coffee_sales_timeline_{self.timestamp}.png')
            plt.savefig(time_plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(time_plot_path)
        
        # 2. Gráfico de top produtos
        if 'top_products' in report_data:
            plt.figure(figsize=(12, 8))
            top_products = list(report_data['top_products'].keys())[:10]
            top_values = list(report_data['top_products'].values())[:10]
            
            bars = plt.bar(range(len(top_products)), top_values, color='skyblue', alpha=0.7)
            plt.title('Top 10 Tipos de Café por Vendas', fontsize=14, fontweight='bold')
            plt.xlabel('Tipos de Café')
            plt.ylabel('Total de Vendas (R$)')
            plt.xticks(range(len(top_products)), top_products, rotation=45, ha='right')
            
            # Adiciona valores nas barras
            for bar, value in zip(bars, top_values):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(top_values)*0.01,
                        f'{value:.0f}', ha='center', va='bottom')
            
            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            
            products_plot_path = os.path.join(self.output_dir, f'top_products_{self.timestamp}.png')
            plt.savefig(products_plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(products_plot_path)
        
        # 3. Distribuição de vendas
        if 'money' in df.columns:
            plt.figure(figsize=(10, 6))
            plt.hist(df['money'], bins=30, alpha=0.7, color='lightgreen', edgecolor='black')
            plt.title('Distribuição de Valores de Vendas de Café', fontsize=14, fontweight='bold')
            plt.xlabel('Valor da Venda (R$)')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            distribution_plot_path = os.path.join(self.output_dir, f'coffee_sales_distribution_{self.timestamp}.png')
            plt.savefig(distribution_plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(distribution_plot_path)
        
        # 4. Heatmap de vendas por tipo de café e mês
        if 'coffee_name' in df.columns and 'Date' in df.columns:
            df['month'] = pd.to_datetime(df['Date']).dt.to_period('M')
            pivot_data = df.pivot_table(values='money', index='coffee_name', columns='month', aggfunc='sum', fill_value=0)
            
            plt.figure(figsize=(12, 8))
            sns.heatmap(pivot_data, annot=True, fmt='.0f', cmap='YlOrRd', cbar_kws={'label': 'Vendas (R$)'})
            plt.title('Heatmap de Vendas por Tipo de Café e Mês', fontsize=14, fontweight='bold')
            plt.xlabel('Mês')
            plt.ylabel('Tipo de Café')
            plt.tight_layout()
            
            heatmap_path = os.path.join(self.output_dir, f'coffee_sales_heatmap_{self.timestamp}.png')
            plt.savefig(heatmap_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(heatmap_path)
        
        logger.info(f"Criadas {len(image_paths)} visualizações")
        return image_paths
    
    def create_pdf_report(self, report_data: Dict, image_paths: List[str], 
                         title: str = "Relatório de Vendas") -> str:
        """
        Cria relatório em PDF
        
        Args:
            report_data (Dict): Dados do relatório
            image_paths (List[str]): Caminhos das imagens
            title (str): Título do relatório
            
        Returns:
            str: Caminho do arquivo PDF criado
        """
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)
        
        # Título
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(0, 10, title, 0, 1, 'C')
        pdf.ln(10)
        
        # Data de geração
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 10, f'Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}', 0, 1, 'C')
        pdf.ln(10)
        
        # Resumo executivo
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Resumo Executivo', 0, 1)
        pdf.set_font('Arial', '', 10)
        
        summary_text = f"""
        Total de Vendas: R$ {report_data.get('total_sales', 0):,.2f}
        Total de Transações: {report_data.get('total_transactions', 0):,}
        Ticket Médio: R$ {report_data.get('average_transaction', 0):,.2f}
        Produtos Únicos: {report_data.get('unique_products', 0):,}
        """
        
        for line in summary_text.strip().split('\n'):
            pdf.cell(0, 6, line.strip(), 0, 1)
        
        pdf.ln(10)
        
        # Adiciona imagens
        for i, image_path in enumerate(image_paths):
            if os.path.exists(image_path):
                pdf.add_page()
                pdf.set_font('Arial', 'B', 12)
                pdf.cell(0, 10, f'Gráfico {i+1}', 0, 1)
                pdf.image(image_path, x=10, y=30, w=190)
        
        # Salva o PDF
        pdf_path = os.path.join(self.output_dir, f'report_{self.timestamp}.pdf')
        pdf.output(pdf_path)
        
        logger.info(f"Relatório PDF criado: {pdf_path}")
        return pdf_path
    
    def create_excel_report(self, df: pd.DataFrame, report_data: Dict) -> str:
        """
        Cria relatório em Excel
        
        Args:
            df (pd.DataFrame): DataFrame com dados
            report_data (Dict): Dados do relatório
            
        Returns:
            str: Caminho do arquivo Excel criado
        """
        excel_path = os.path.join(self.output_dir, f'report_{self.timestamp}.xlsx')
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Dados originais
            df.to_excel(writer, sheet_name='Dados_Originais', index=False)
            
            # Resumo
            summary_df = pd.DataFrame([
                ['Total de Vendas', report_data.get('total_sales', 0)],
                ['Total de Transações', report_data.get('total_transactions', 0)],
                ['Ticket Médio', report_data.get('average_transaction', 0)],
                ['Produtos Únicos', report_data.get('unique_products', 0)]
            ], columns=['Métrica', 'Valor'])
            summary_df.to_excel(writer, sheet_name='Resumo', index=False)
            
            # Top produtos
            if 'top_products' in report_data:
                top_products_df = pd.DataFrame(
                    list(report_data['top_products'].items()),
                    columns=['Produto', 'Total_Vendas']
                )
                top_products_df.to_excel(writer, sheet_name='Top_Produtos', index=False)
            
            # Vendas mensais
            if 'monthly_sales' in report_data:
                monthly_df = pd.DataFrame(
                    list(report_data['monthly_sales'].items()),
                    columns=['Mês', 'Vendas']
                )
                monthly_df.to_excel(writer, sheet_name='Vendas_Mensais', index=False)
        
        logger.info(f"Relatório Excel criado: {excel_path}")
        return excel_path
    
    def send_email_report(self, pdf_path: str, excel_path: str, image_paths: List[str]) -> bool:
        """
        Envia relatório por email
        
        Args:
            pdf_path (str): Caminho do arquivo PDF
            excel_path (str): Caminho do arquivo Excel
            image_paths (List[str]): Lista de caminhos das imagens
            
        Returns:
            bool: True se enviado com sucesso, False caso contrário
        """
        if not self.email_config.is_configured():
            logger.warning("Configurações de email não preenchidas. Email não será enviado.")
            logger.info("Para configurar email, edite a classe EmailConfig no arquivo report.py")
            return False
        
        try:
            # Cria mensagem
            msg = MIMEMultipart()
            msg['From'] = self.email_config.sender_email
            
            # Converte lista de emails em string para o cabeçalho
            recipient_emails = self.email_config.recipient_email
            if isinstance(recipient_emails, list):
                msg['To'] = ', '.join(recipient_emails)
            else:
                msg['To'] = recipient_emails
                
            msg['Subject'] = self.email_config.get_subject()
            
            # Adiciona corpo do email
            email_body = self.email_config.get_body()
            if isinstance(email_body, list):
                email_body = '\n'.join(email_body)
            msg.attach(MIMEText(email_body, 'plain', 'utf-8'))
            
            # Anexa PDF
            if os.path.exists(pdf_path):
                with open(pdf_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(pdf_path)}'
                    )
                    msg.attach(part)
            
            # Anexa Excel
            if os.path.exists(excel_path):
                with open(excel_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(excel_path)}'
                    )
                    msg.attach(part)
            
            # Anexa imagens (opcional - pode ser pesado)
            for image_path in image_paths[:3]:  # Limita a 3 imagens para não sobrecarregar
                if os.path.exists(image_path):
                    with open(image_path, "rb") as attachment:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {os.path.basename(image_path)}'
                        )
                        msg.attach(part)
            
            # Conecta e envia email
            server = smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port)
            server.starttls()
            server.login(self.email_config.sender_email, self.email_config.sender_password)
            
            text = msg.as_string()
            
            # Converte string de emails em lista ou usa lista diretamente
            recipient_emails = self.email_config.recipient_email
            if isinstance(recipient_emails, str):
                recipient_list = [email.strip() for email in recipient_emails.split(',')]
            else:
                recipient_list = recipient_emails
            
            server.sendmail(self.email_config.sender_email, recipient_list, text)
            server.quit()
            
            logger.info(f"Email enviado com sucesso para {', '.join(recipient_list)}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao enviar email: {str(e)}")
            return False
    
    def generate_complete_report(self, df: pd.DataFrame, send_email: bool = True) -> Dict[str, str]:
        """
        Gera relatório completo (PDF, Excel e visualizações)
        
        Args:
            df (pd.DataFrame): DataFrame com dados
            send_email (bool): Se deve enviar email automaticamente
            
        Returns:
            Dict[str, str]: Dicionário com caminhos dos arquivos gerados
        """
        # Cria dados do relatório
        report_data = self.create_sales_summary_report(df)
        
        # Cria visualizações
        image_paths = self.create_visualizations(df, report_data)
        
        # Cria PDF
        pdf_path = self.create_pdf_report(report_data, image_paths)
        
        # Cria Excel
        excel_path = self.create_excel_report(df, report_data)
        
        # Envia email se solicitado
        email_sent = False
        if send_email:
            email_sent = self.send_email_report(pdf_path, excel_path, image_paths)
        
        return {
            'pdf': pdf_path,
            'excel': excel_path,
            'images': image_paths,
            'report_data': report_data,
            'email_sent': email_sent
        }

def main():
    """Função principal para teste do módulo"""
    generator = ReportGenerator()
    
    try:
        # Carrega dados
        df = pd.read_csv('data/Coffe_sales.csv')
        print(f"Dados carregados. Shape: {df.shape}")
        
        # Gera relatório completo
        results = generator.generate_complete_report(df)
        
        print("Relatórios gerados:")
        print(f"  PDF: {results['pdf']}")
        print(f"  Excel: {results['excel']}")
        print(f"  Imagens: {len(results['images'])} arquivos")
        
        # Mostra status do email
        if results['email_sent']:
            print(f"  ✅ Email enviado com sucesso!")
        else:
            print(f"  ⚠️  Email não enviado (verifique configurações)")
        
        # Mostra resumo dos dados
        report_data = results['report_data']
        print(f"\nResumo:")
        print(f"  Total de Vendas: R$ {report_data.get('total_sales', 0):,.2f}")
        print(f"  Total de Transações: {report_data.get('total_transactions', 0):,}")
        print(f"  Ticket Médio: R$ {report_data.get('average_transaction', 0):,.2f}")
        
    except FileNotFoundError:
        print("Arquivo de dados não encontrado. Execute primeiro o extract.py")

if __name__ == "__main__":
    main()
