use anyhow::Context;
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Tokio1Executor,
    message::{Mailbox, Message},
    transport::smtp::authentication::Credentials,
};

#[derive(Clone)]
pub enum Mailer {
    Disabled,
    Smtp {
        from: Mailbox,
        transport: AsyncSmtpTransport<Tokio1Executor>,
    },
}

impl Mailer {
    pub fn from_env() -> anyhow::Result<Self> {
        let host = std::env::var("SMTP_HOST").ok().filter(|s| !s.is_empty());
        let from = std::env::var("EMAIL_FROM").ok().filter(|s| !s.is_empty());

        if host.is_none() || from.is_none() {
            tracing::warn!("SMTP_HOST or EMAIL_FROM not set; email sending disabled");
            return Ok(Self::Disabled);
        }

        let host = host.unwrap();
        let from: Mailbox = from.unwrap().parse().context("invalid EMAIL_FROM value")?;

        let port = std::env::var("SMTP_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(587);

        let username = std::env::var("SMTP_USERNAME").ok();
        let password = std::env::var("SMTP_PASSWORD").ok();

        let mut builder = lettre::AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&host)
            .context("build SMTP transport")?;
        builder = builder.port(port);

        if let (Some(user), Some(pass)) = (username, password) {
            builder = builder.credentials(Credentials::new(user, pass));
        }

        let transport = builder.build();

        Ok(Self::Smtp { from, transport })
    }

    pub fn disabled() -> Self {
        Self::Disabled
    }

    pub async fn send(&self, to: &str, subject: &str, body: &str) -> anyhow::Result<()> {
        match self {
            Mailer::Disabled => {
                tracing::info!(%to, %subject, "email sending disabled; token logged only");
                Ok(())
            }
            Mailer::Smtp { from, transport } => {
                let to: Mailbox = to.parse().context("invalid recipient email")?;
                let email = Message::builder()
                    .from(from.clone())
                    .to(to)
                    .subject(subject)
                    .body(body.to_string())
                    .context("build email message")?;
                transport.clone().send(email).await.context("send email")?;
                Ok(())
            }
        }
    }
}
