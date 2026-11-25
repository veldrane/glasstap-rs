use crate::app::AppState;
use crate::httpevents::*;
//use crate::collector::Message;
use bytes::Bytes;
use crate::prelude::*;
use uuid::Uuid;


pub struct Context {
    request_id: Uuid,
    upstream_addr: Option<SocketAddr>,
}

// Implement the ProxyHttp trait for LB - Main request http flow!
#[async_trait]
impl ProxyHttp for AppState{

    type CTX = Context;
    
    fn new_ctx(&self) -> Self::CTX {

        Context { request_id: Uuid::new_v4(), upstream_addr: None }
    }

    async fn request_filter(&self, session: &mut Session,  ctx: &mut Self::CTX) -> Result<bool> {

    let req = session.req_header();
    let client_addr = match session.client_addr() {
        Some(addr) => addr,
        None => {
            println!("No client address found, treating as Zone None");
            return Ok(false);
        }
    };

    self.tx.send(HttpEvent::Request(RequestEvent {
            request_id: ctx.request_id.to_string(),
            client_addr: client_addr.to_string(),
            method: req.method.to_string(),
            uri: req.uri.to_string(),
            headers: req.headers.iter().map(|(name, value)| Header {
                name: name.to_string(),
                value: String::from_utf8_lossy(value.as_bytes()).to_string()
            }).collect(),
        })).await.unwrap();

        Ok(false)
    }


    async fn request_body_filter( &self, _session: &mut Session, body: &mut Option<Bytes>, end_of_stream: bool, ctx: &mut Self::CTX,) -> Result<()> {

    if let Some(chunk) = body.as_ref() {
        let message = format!(
            "{}",
            String::from_utf8_lossy(chunk)
        );
        let _ = self.tx.send(HttpEvent::RequestBody(BodyEvent {
            request_id: ctx.request_id.to_string(),
            size: chunk.len(),
            end_of_stream,
            content: Some(message),
        })).await;
    }

    if end_of_stream {
        let _ = self.tx.send(HttpEvent::RequestBody(BodyEvent {
            request_id: ctx.request_id.to_string(),
            size: 0,
            end_of_stream: true,
            content: None,
        })).await;
    }

    Ok(())
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

    async fn upstream_request_filter(&self,_session: &mut Session, _upstream_request: &mut RequestHeader, _ctx: &mut Self::CTX,
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


        self.tx.send(HttpEvent::Response(ResponseEvent {
            request_id: ctx.request_id.to_string(),
            upstream_addr: upstream.to_string(),
            status: upstream_response.status.as_u16(),
            headers: upstream_response.headers.iter().map(|(name, value)| Header {
                name: name.to_string(),
                value: String::from_utf8_lossy(value.as_bytes()).to_string()
            }).collect(),
        })).await.unwrap();

        Ok(())
    }


    fn response_body_filter(&self,_session: &mut Session, body: &mut Option<Bytes>, end_of_stream: bool, ctx: &mut Self::CTX) -> Result<Option<Duration>> {

        if let Some(chunk) = body.as_ref() {
            let message = format!("{}",String::from_utf8_lossy(chunk)
        );

            //let _ = self.tx.try_send(Message(message));
            let _ = self.tx.try_send(
                HttpEvent::ResponseBody(BodyEvent {
                    request_id: ctx.request_id.to_string(),
                    size: chunk.len(),
                    end_of_stream,
                    content: Some(message),
                })
            );
        }

        if end_of_stream {
            let _ = self.tx.try_send(
                HttpEvent::ResponseBody(BodyEvent {
                    request_id: ctx.request_id.to_string(),
                    size: 0,
                    end_of_stream: true,
                    content: None,
                })
            );  
        }

        Ok(None)
    }

}


// Implement the BackgroundService trait for upstream target discovery and load balancer updates


