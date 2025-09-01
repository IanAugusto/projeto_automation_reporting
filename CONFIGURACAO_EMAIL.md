# 📧 Configuração de Email - Gmail

## 🔧 Como configurar o envio de email

### 1. **Edite o arquivo `pandas_version/report.py`**

Encontre a classe `EmailConfig` (linhas 29-73) e preencha os campos:

```python
class EmailConfig:
    def __init__(self):
        # ===== CONFIGURAÇÕES GMAIL - PREENCHA COM SEUS DADOS =====
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = "seuemail@gmail.com"  # ← SEU EMAIL GMAIL AQUI
        self.sender_password = "sua_app_password"  # ← SUA APP PASSWORD AQUI
        self.recipient_email = "destinatario@email.com"  # ← EMAIL DESTINATÁRIO AQUI
```

### 2. **Criar App Password no Gmail**

1. Acesse: https://myaccount.google.com/
2. Vá em **Segurança** → **Verificação em duas etapas**
3. Ative a verificação em duas etapas (se não estiver ativa)
4. Vá em **Senhas de app**
5. Selecione **Outro (nome personalizado)**
6. Digite: "Relatórios Automatizados"
7. **Copie a senha gerada** (16 caracteres)
8. Cole no campo `sender_password`

### 3. **Exemplo de configuração:**

```python
self.sender_email = "meuemail@gmail.com"
self.sender_password = "abcd efgh ijkl mnop"  # App Password do Gmail
self.recipient_email = "gerente@empresa.com"
```

### 4. **Testar a configuração:**

```bash
cd pandas_version
python report.py
```

## ✅ **O que será enviado por email:**

- **PDF** com relatório completo
- **Excel** com dados detalhados
- **Gráficos PNG** (até 3 imagens)
- **Corpo do email** com resumo

## 🔒 **Segurança:**

- ✅ Usa App Password (não sua senha normal)
- ✅ Conexão criptografada (TLS)
- ✅ Credenciais ficam no código (apenas local)

## ⚠️ **Limites Gmail:**

- **500 emails/dia** (gratuito)
- **25MB por email** (suficiente para relatórios)
- **Sem custo adicional**

## 🚨 **Problemas comuns:**

1. **"Login failed"** → Verifique App Password
2. **"Less secure apps"** → Use App Password, não senha normal
3. **"Connection refused"** → Verifique internet/firewall

## 📞 **Suporte:**

Se tiver problemas, verifique:
- ✅ Verificação em duas etapas ativa
- ✅ App Password criada corretamente
- ✅ Email e senha copiados sem espaços extras
