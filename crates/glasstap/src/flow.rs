use crate::app::AppState;
use crate::collector::Message;
use bytes::Bytes;
use crate::prelude::*;


pub struct Context {
    upstream_addr: Option<SocketAddr>,
}

// Implement the ProxyHttp trait for LB - Main request http flow!
#[async_trait]
impl ProxyHttp for AppState {

    type CTX = Context;
    
    fn new_ctx(&self) -> Self::CTX {
        Context { upstream_addr: None }
    }

    async fn request_filter(&self, session: &mut Session,  _ctx: &mut Self::CTX) -> Result<bool> {

    let req = session.req_header();
    let client_addr = match session.client_addr() {
        Some(addr) => addr,
        None => {
            println!("No client address found, treating as Zone None");
            return Ok(false);
        }
    };

    let headers = req.headers.iter()
        .map(|(name, value)| {
            format!("- {}: {}", name, String::from_utf8_lossy(value.as_bytes()))
        })
        .collect::<Vec<_>>()
        .join("\n");

    let message = format!(
        "\nMethod: {}\nURL: {}\nVersion: {:?}\nHeaders:\n{}",
        req.method,
        req.uri,
        req.version,
        headers,
    );

        self.tx.send(Message(format!("\n---\nRequest\n\nClient: {} {}",client_addr, message))).await.unwrap();

        let _socket_addr = match session.client_addr() {
            Some(addr) => addr,
            None => {
                println!("No client address found, treating as Zone None");
                return Ok(false);
            }
        };

        Ok(false)
    }

    async fn upstream_peer( &self, _session: &mut Session, ctx: &mut Self::CTX) -> Result<Box<HttpPeer>> {


        let upstream = {
            let lb = self.lb.load();
            match lb.select(b"", 256) {
                Some(upstream) => upstream,
                None => return Err(Error::new(ErrorType::HTTPStatus(504)))
            }
        };
        
        ctx.upstream_addr = Some(upstream.clone().as_inet().unwrap().clone());

        let peer = Box::new(HttpPeer::new(upstream, false, "".to_string()));
        Ok(peer)
    }

    async fn upstream_request_filter(&self,_session: &mut Session, upstream_request: &mut RequestHeader,ctx: &mut Self::CTX,
    ) -> Result<()>
    {
        Ok(())
    }

    async fn response_filter(&self, _session: &mut Session, upstream_response: &mut ResponseHeader, ctx: &mut Self::CTX) -> Result<()> {

        let upstream = match ctx.upstream_addr {
            Some(addr) => addr,
            None => {
                println!("No upstream address found");
                return Ok(());
            }
        };

        let headers = upstream_response.headers.iter()
            .map(|(name, value)| {
                format!("- {}: {}", name, String::from_utf8_lossy(value.as_bytes()))
            })
            .collect::<Vec<_>>()
            .join("\n");

        let message = format!(
            "\nVersion: {:?}\nStatus: {}\nHeaders:\n{}",
            upstream_response.version,
            upstream_response.status,
            headers,
        );

        self.tx.send(Message(format!("\nResponse\n\nUpstream: {}\n{}",upstream, message))).await.unwrap();

        Ok(())
    }


    fn response_body_filter(&self,_session: &mut Session, body: &mut Option<Bytes>, end_of_stream: bool, _ctx: &mut Self::CTX) -> Result<Option<Duration>> {

        if let Some(chunk) = body.as_ref() {
            let message = format!(
                "\n\nResponse body chunk: {} bytes\n\n{}",
                chunk.len(),
                String::from_utf8_lossy(chunk)
            );

            let _ = self.tx.try_send(Message(message));
        }

        if end_of_stream {
            let _ = self.tx.try_send(
                Message("\nResponse body completed".into())
            );
        }

        Ok(None)
    }

}


// Implement the BackgroundService trait for upstream target discovery and load balancer updates


