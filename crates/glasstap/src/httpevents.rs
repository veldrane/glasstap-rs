use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum HttpEvent {
    Request(RequestEvent),
    Response(ResponseEvent),
    RequestBody(BodyEvent),
    ResponseBody(BodyEvent),
}

#[derive(Debug, Serialize)]
pub struct RequestEvent {
    pub request_id: String,
    pub client_addr: String,
    pub method: String,
    pub uri: String,
    pub headers: Vec<Header>,
}

#[derive(Debug, Serialize)]
pub struct ResponseEvent {
    pub request_id: String,
    pub upstream_addr: String,
    pub status: u16,
    pub headers: Vec<Header>,
}

#[derive(Debug, Serialize)]
pub struct BodyEvent {
    pub request_id: String,
    pub size: usize,
    pub end_of_stream: bool,
    pub content: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Header {
    pub name: String,
    pub value: String,
}