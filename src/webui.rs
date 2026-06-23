use axum::{
    extract::{State, WebSocketUpgrade, ws::{Message,WebSocket}},
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use gilrs::{Button, Event, Gilrs};
use rdev::{grab, Event as rEvent, EventType, Key};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    thread,
};
use tokio::{net::TcpListener, runtime::Runtime, sync::RwLock};

// ========== 平台信号处理 ==========
#[cfg(unix)]
pub use {
    signal_hook::consts::SIGINT,
    signal_hook::iterator::Signals,
};
#[cfg(windows)]
use ctrlc;

// ========== 常量定义 ==========
const THROTTLE_MS: u64 = 200;
const KEY_POLL_SLEEP: Duration = Duration::from_millis(30);
const WEB_PORT: u16 = 8090;
const API_CONFIG_NAME: &str = "config.txt";
const KEYMAP_CONFIG_NAME: &str = "keymap.txt";

// ========== 事件推送结构体（WebSocket统一消息格式） ==========
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum WsEvent {
    Keyboard { key: String, is_release: bool },
    Gamepad { btn: String, is_release: bool },
}

// WebSocket 客户端池
type WsClients = Arc<RwLock<Vec<futures_util::stream::SplitSink<WebSocket, Message>>>>;

// ========== 数据模型 ==========
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub api_list: Vec<String>,
    pub default_index: Option<usize>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KeyMapConfig {
    pub map: HashMap<String, HashSet<String>>,
}

#[derive(Clone)]
pub struct AppState {
    pub api_cfg: Arc<RwLock<AppConfig>>,
    pub keymap_cfg: Arc<RwLock<KeyMapConfig>>,
    pub target_api: Arc<RwLock<Option<String>>>,
    pub rt: Arc<Runtime>,
    // WebSocket 客户端连接池
    pub ws_clients: WsClients,
}

#[derive(Deserialize)]
pub struct ApiEditReq {
    pub api_list: Vec<String>,
    pub default_idx: Option<usize>,
}

#[derive(Deserialize)]
pub struct KeymapEditReq {
    pub action: String,
    pub keys: Vec<String>,
}

#[derive(Serialize)]
pub struct ActionEvent {
    pub action: String,
    pub timestamp: u128,
}

// ========== 广播所有WS客户端 ==========
async fn broadcast_ws(state: AppState, evt: WsEvent) {
    let json = match serde_json::to_string(&evt) {
        Ok(s) => s,
        Err(_) => return,
    };
    let msg = Message::Text(json.into());
    let mut clients = state.ws_clients.write().await;
    let mut dead = Vec::new();
    for (idx, sink) in clients.iter_mut().enumerate() {
        if sink.send(msg.clone()).await.is_err() {
            dead.push(idx);
        }
    }
    // 清理断开的连接
    for &idx in dead.iter().rev() {
        clients.remove(idx);
    }
}

// ========== 浏览器JS键名 → rdev标准键名转换表（修复&str返回错误） ==========
fn web_key_to_rdev_name(web_key: &str) -> String {
    match web_key {
        "ArrowUp" => "Up".to_string(),
        "ArrowDown" => "Down".to_string(),
        "ArrowLeft" => "Left".to_string(),
        "ArrowRight" => "Right".to_string(),
        " " => "Space".to_string(),
        "Enter" => "Return".to_string(),
        "Backspace" => "Backspace".to_string(),
        "Escape" => "Esc".to_string(),
        "Tab" => "Tab".to_string(),
        "ShiftLeft" | "Shift" => "ShiftLeft".to_string(),
        "ShiftRight" => "ShiftRight".to_string(),
        "Control" | "ControlLeft" => "ControlLeft".to_string(),
        "ControlRight" => "ControlRight".to_string(),
        "Alt" => "Alt".to_string(),
        "Meta" | "MetaLeft" => "MetaLeft".to_string(),
        "MetaRight" => "MetaRight".to_string(),
        "PrintScreen" => "PrintScreen".to_string(),
        "Home" => "Home".to_string(),
        "End" => "End".to_string(),
        "PageUp" => "PageUp".to_string(),
        "PageDown" => "PageDown".to_string(),
        "Insert" => "Insert".to_string(),
        "Delete" => "Delete".to_string(),
        "CapsLock" => "CapsLock".to_string(),
        "NumLock" => "NumLock".to_string(),
        "ScrollLock" => "ScrollLock".to_string(),
        "Pause" => "Pause".to_string(),
        "F1" | "F2" | "F3" | "F4" | "F5" | "F6" | "F7" | "F8" | "F9" | "F10" | "F11" | "F12" => web_key.to_string(),
        "0" => "Num0".to_string(),
        "1" => "Num1".to_string(),
        "2" => "Num2".to_string(),
        "3" => "Num3".to_string(),
        "4" => "Num4".to_string(),
        "5" => "Num5".to_string(),
        "6" => "Num6".to_string(),
        "7" => "Num7".to_string(),
        "8" => "Num8".to_string(),
        "9" => "Num9".to_string(),
        "-" => "Minus".to_string(),
        "=" => "Equal".to_string(),
        "[" => "LeftBracket".to_string(),
        "]" => "RightBracket".to_string(),
        ";" => "SemiColon".to_string(),
        "'" => "Quote".to_string(),
        "\\" => "BackSlash".to_string(),
        "," => "Comma".to_string(),
        "." => "Dot".to_string(),
        "/" => "Slash".to_string(),
        "`" => "BackQuote".to_string(),
        k if k.len() == 1 && k.chars().all(|c| c.is_ascii_uppercase()) => format!("Key{}", k),
        k if k.len() == 1 && k.chars().all(|c| c.is_ascii_lowercase()) => format!("Key{}", k.to_uppercase()),
        other => other.to_string(),
    }
}

// rdev Key枚举转统一字符串
fn process_key(key_enum: &Key) -> String {
    match key_enum {
        Key::LeftArrow => "Left".to_string(),
        Key::RightArrow => "Right".to_string(),
        Key::UpArrow => "Up".to_string(),
        Key::DownArrow => "Down".to_string(),
        Key::PrintScreen => "PrintScreen".to_string(),
        Key::Space => "Space".to_string(),
        Key::Home => "Home".to_string(),
        Key::Alt => "Alt".to_string(),
        Key::AltGr => "AltGr".to_string(),
        Key::Backspace => "Backspace".to_string(),
        Key::CapsLock => "CapsLock".to_string(),
        Key::ControlLeft => "ControlLeft".to_string(),
        Key::ControlRight => "ControlRight".to_string(),
        Key::Delete => "Delete".to_string(),
        Key::End => "End".to_string(),
        Key::Escape => "Esc".to_string(),
        Key::F1 => "F1".to_string(),
        Key::F10 => "F10".to_string(),
        Key::F11 => "F11".to_string(),
        Key::F12 => "F12".to_string(),
        Key::F2 => "F2".to_string(),
        Key::F3 => "F3".to_string(),
        Key::F4 => "F4".to_string(),
        Key::F5 => "F5".to_string(),
        Key::F6 => "F6".to_string(),
        Key::F7 => "F7".to_string(),
        Key::F8 => "F8".to_string(),
        Key::F9 => "F9".to_string(),
        Key::MetaLeft => "MetaLeft".to_string(),
        Key::MetaRight => "MetaRight".to_string(),
        Key::PageDown => "PageDown".to_string(),
        Key::PageUp => "PageUp".to_string(),
        Key::Return => "Return".to_string(),
        Key::ShiftLeft => "ShiftLeft".to_string(),
        Key::ShiftRight => "ShiftRight".to_string(),
        Key::Tab => "Tab".to_string(),
        Key::ScrollLock => "ScrollLock".to_string(),
        Key::Pause => "Pause".to_string(),
        Key::NumLock => "NumLock".to_string(),
        Key::BackQuote => "BackQuote".to_string(),
        Key::Num1 => "Num1".to_string(),
        Key::Num2 => "Num2".to_string(),
        Key::Num3 => "Num3".to_string(),
        Key::Num4 => "Num4".to_string(),
        Key::Num5 => "Num5".to_string(),
        Key::Num6 => "Num6".to_string(),
        Key::Num7 => "Num7".to_string(),
        Key::Num8 => "Num8".to_string(),
        Key::Num9 => "Num9".to_string(),
        Key::Num0 => "Num0".to_string(),
        Key::Minus => "Minus".to_string(),
        Key::Equal => "Equal".to_string(),
        Key::KeyQ => "KeyQ".to_string(),
        Key::KeyW => "KeyW".to_string(),
        Key::KeyE => "KeyE".to_string(),
        Key::KeyR => "KeyR".to_string(),
        Key::KeyT => "KeyT".to_string(),
        Key::KeyY => "KeyY".to_string(),
        Key::KeyU => "KeyU".to_string(),
        Key::KeyI => "KeyI".to_string(),
        Key::KeyO => "KeyO".to_string(),
        Key::KeyP => "KeyP".to_string(),
        Key::LeftBracket => "LeftBracket".to_string(),
        Key::RightBracket => "RightBracket".to_string(),
        Key::KeyA => "KeyA".to_string(),
        Key::KeyS => "KeyS".to_string(),
        Key::KeyD => "KeyD".to_string(),
        Key::KeyF => "KeyF".to_string(),
        Key::KeyG => "KeyG".to_string(),
        Key::KeyH => "KeyH".to_string(),
        Key::KeyJ => "KeyJ".to_string(),
        Key::KeyK => "KeyK".to_string(),
        Key::KeyL => "KeyL".to_string(),
        Key::SemiColon => "SemiColon".to_string(),
        Key::Quote => "Quote".to_string(),
        Key::BackSlash => "BackSlash".to_string(),
        Key::IntlBackslash => "IntlBackslash".to_string(),
        Key::KeyZ => "KeyZ".to_string(),
        Key::KeyX => "KeyX".to_string(),
        Key::KeyC => "KeyC".to_string(),
        Key::KeyV => "KeyV".to_string(),
        Key::KeyB => "KeyB".to_string(),
        Key::KeyN => "KeyN".to_string(),
        Key::KeyM => "KeyM".to_string(),
        Key::Comma => "Comma".to_string(),
        Key::Dot => "Dot".to_string(),
        Key::Slash => "Slash".to_string(),
        Key::Insert => "Insert".to_string(),
        Key::KpReturn => "KpReturn".to_string(),
        Key::KpMinus => "KpMinus".to_string(),
        Key::KpPlus => "KpPlus".to_string(),
        Key::KpMultiply => "KpMultiply".to_string(),
        Key::KpDivide => "KpDivide".to_string(),
        Key::Kp0 => "Kp0".to_string(),
        Key::Kp1 => "Kp1".to_string(),
        Key::Kp2 => "Kp2".to_string(),
        Key::Kp3 => "Kp3".to_string(),
        Key::Kp4 => "Kp4".to_string(),
        Key::Kp5 => "Kp5".to_string(),
        Key::Kp6 => "Kp6".to_string(),
        Key::Kp7 => "Kp7".to_string(),
        Key::Kp8 => "Kp8".to_string(),
        Key::Kp9 => "Kp9".to_string(),
        Key::KpDelete => "KpDelete".to_string(),
        Key::Function => "Function".to_string(),
        Key::Unknown(u32) => u32.to_string(),
    }
}

// gilrs Button → 前端展示字符串（修复不存在枚举变体）
fn gamepad_btn_to_str(btn: Button) -> String {
    match btn {
        Button::South => "South(A/Cross)".to_string(),
        Button::East => "East(B/Circle)".to_string(),
        Button::North => "North(X/Triangle)".to_string(),
        Button::West => "West(Y/Square)".to_string(),
        Button::LeftTrigger => "LeftTrigger".to_string(),
        Button::RightTrigger => "RightTrigger".to_string(),
        // Button::L1 => "L1".to_string(),
        // Button::R1 => "R1".to_string(),
        Button::Select => "Select".to_string(),
        Button::Start => "Start".to_string(),
        Button::DPadUp => "DPadUp".to_string(),
        Button::DPadDown => "DPadDown".to_string(),
        Button::DPadLeft => "DPadLeft".to_string(),
        Button::DPadRight => "DPadRight".to_string(),
        // Button::L3 => "LeftStickClick".to_string(),
        // Button::R3 => "RightStickClick".to_string(),
        _ => format!("Unknown({:?})", btn),
    }
}

// ========== 配置文件读写 ==========
fn get_api_config_path() -> PathBuf {
    let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    let dir = exe_path.parent().unwrap_or_else(|| Path::new("."));
    dir.join(API_CONFIG_NAME)
}

fn load_api_config() -> AppConfig {
    let path = get_api_config_path();
    if !path.exists() {
        return AppConfig::default();
    }
    let content = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return AppConfig::default(),
    };
    let mut lines = content.lines();
    let default_index = lines
        .next()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&v| v != 9999);
    let mut api_list = Vec::new();
    for line in lines {
        let url = line.trim().to_string();
        if !url.is_empty() {
            api_list.push(url);
        }
    }
    AppConfig { api_list, default_index }
}

fn save_api_config(cfg: &AppConfig) -> std::io::Result<()> {
    let path = get_api_config_path();
    let mut content = String::new();
    let idx = cfg.default_index.unwrap_or(9999);
    content.push_str(&format!("{}\n", idx));
    for url in &cfg.api_list {
        content.push_str(url);
        content.push('\n');
    }
    std::fs::write(path, content)
}

fn get_keymap_path() -> PathBuf {
    let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    let dir = exe_path.parent().unwrap_or_else(|| Path::new("."));
    dir.join(KEYMAP_CONFIG_NAME)
}

fn load_keymap() -> KeyMapConfig {
    let path = get_keymap_path();
    if !path.exists() {
        let mut map = HashMap::new();
        map.insert("prev_page".into(), HashSet::from(["Up".into(), "Left".into()]));
        map.insert("next_page".into(), HashSet::from(["Down".into(), "Right".into()]));
        map.insert("brightness".into(), HashSet::from(["KpPlus".into()]));
        return KeyMapConfig { map };
    }
    let content = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return KeyMapConfig::default(),
    };
    let mut map = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() != 2 {
            continue;
        }
        let action = parts[0].trim().to_string();
        let keys: HashSet<String> = parts[1]
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !keys.is_empty() {
            map.insert(action, keys);
        }
    }
    KeyMapConfig { map }
}

fn save_keymap(km: &KeyMapConfig) -> std::io::Result<()> {
    let path = get_keymap_path();
    let mut buf = String::new();
    for (action, keys) in &km.map {
        let key_str: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        buf.push_str(&format!("{}: {}\n", action, key_str.join(",")));
    }
    std::fs::write(path, buf)
}

// ========== 退出信号 ==========
fn setup_exit_handler() {
    #[cfg(unix)]
    {
        thread::spawn(|| {
            let mut signals = Signals::new(&[SIGINT]).unwrap();
            for _ in signals.forever() {
                eprintln!("\n🛑 Ctrl+C 退出");
                std::process::exit(0);
            }
        });
    }
    #[cfg(windows)]
    {
        ctrlc::set_handler(|| {
            eprintln!("\n🛑 Ctrl+C 退出");
            std::process::exit(0);
        })
        .unwrap();
    }
}

// ========== 发送HTTP请求（复用全局runtime） ==========
fn send_action_request_blocking(state: AppState, api_url: String, action: String) {
    state.rt.spawn(async move {
        let client = reqwest::Client::new();
        let payload = ActionEvent {
            action: action.clone(),
            timestamp: Instant::now().elapsed().as_millis(),
        };
        match client.post(&api_url).json(&payload).send().await {
            Ok(_res) => println!("✅ HTTP推送成功 | action={}", action),
            Err(e) => eprintln!("❌ HTTP推送失败 | url={}, err={}", api_url, e),
        }
    });
}

// ========== 全局键鼠捕获线程（WS广播按键事件） ==========
fn start_keyboard_listener(state: AppState) {
    thread::spawn(move || {
        println!("⌨️ 全局键鼠捕获线程启动");
        let throttle_lock = Arc::new(Mutex::new(HashMap::<String, Instant>::new()));
        let rt_clone = state.rt.clone();

        let callback = move |event: rEvent| -> Option<rEvent> {
            match &event.event_type {
                EventType::KeyPress(key) => {
                    let name = process_key(key);
                    rt_clone.block_on(broadcast_ws(state.clone(), WsEvent::Keyboard { key: name, is_release: false }));
                }
                EventType::KeyRelease(key) => {
                    let key_name = process_key(key);
                    println!("[按键松开] {}", key_name);
                    // WS推送松开事件
                    rt_clone.block_on(broadcast_ws(state.clone(), WsEvent::Keyboard { key: key_name.clone(), is_release: true }));

                    let km = rt_clone.block_on(state.keymap_cfg.read()).clone();
                    let target_api_opt = rt_clone.block_on(state.target_api.read()).clone();
                    let Some(target_url) = target_api_opt else {
                        eprintln!("⚠️ 未选中目标API，跳过推送");
                        return Some(event);
                    };

                    let mut throttle_map = throttle_lock.lock().unwrap();
                    let now = Instant::now();
                    for (action, bind_keys) in &km.map {
                        if bind_keys.contains(&key_name) {
                            let last = throttle_map.entry(action.clone()).or_insert(now);
                            if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                                println!("⏱️ 节流过滤: {} 冷却中", action);
                                continue;
                            }
                            *last = now;
                            println!("🔥 匹配动作 {} | 按键 {}", action, key_name);
                            send_action_request_blocking(state.clone(), target_url.clone(), action.clone());
                        }
                    }
                }
                EventType::ButtonRelease(_btn) => {}
                _ => {}
            }
            Some(event)
        };

        match grab(callback) {
            Ok(_) => {}
            Err(e) => eprintln!("❌ rdev捕获失败: {:?}", e),
        }
    });
}

// ========== 手柄监听线程（WS实时广播手柄按键） ==========
fn start_gamepad_listener(state: AppState) {
    thread::spawn(move || {
        println!("🎮 手柄监听线程启动");
        let mut gilrs = match Gilrs::new() {
            Ok(g) => g,
            Err(e) => {
                eprintln!("❌ 手柄初始化失败: {:?}", e);
                return;
            }
        };
        let mut pad_throttle = HashMap::<Button, Instant>::new();
        let rt_clone = state.rt.clone();

        loop {
            while let Some(Event { event, .. }) = gilrs.next_event() {
                match event {
                    gilrs::EventType::ButtonPressed(btn, _) => {
                        let name = gamepad_btn_to_str(btn);
                        rt_clone.block_on(broadcast_ws(state.clone(), WsEvent::Gamepad { btn: name, is_release: false }));
                    }
                    gilrs::EventType::ButtonReleased(btn, _) => {
                        let btn_name = gamepad_btn_to_str(btn);
                        rt_clone.block_on(broadcast_ws(state.clone(), WsEvent::Gamepad { btn: btn_name.clone(), is_release: true }));

                        if btn == Button::South {
                            let now = Instant::now();
                            let last = pad_throttle.entry(Button::South).or_insert(now);
                            if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                                continue;
                            }
                            *last = now;
                            let target_api_opt = rt_clone.block_on(state.target_api.read()).clone();
                            let Some(url) = target_api_opt else {
                                eprintln!("⚠️ 手柄：无目标API");
                                continue;
                            };
                            println!("🎮 手柄South键触发 next_page");
                            send_action_request_blocking(state.clone(), url, "next_page".to_string());
                        }
                    }
                    _ => {}
                }
            }
            thread::sleep(KEY_POLL_SLEEP);
        }
    });
}

// ========== WebSocket 连接处理路由 ==========
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl axum::response::IntoResponse {
    let resp = ws.on_upgrade(move |socket| async move {
        let (sink, mut stream) = socket.split();
        // 加入客户端池
        state.ws_clients.write().await.push(sink);
        // 丢弃前端发来的消息（单向推送，前端只收不发）
        while stream.next().await.is_some() {}
    });
    resp
}

// ========== Web页面（内置WS实时展示键盘+手柄） ==========
async fn index_page() -> Html<String> {
    let html = r#"
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>键鼠手柄映射配置面板 | WebSocket实时</title>
<style>
*{box-sizing:border-box;font-family:system-ui}
body{max-width:1200px;margin:20px auto;padding:0 20px}
.card{border:1px solid #ddd;border-radius:8px;padding:16px;margin-bottom:20px}
h3{margin-top:0}
.row{display:flex;gap:10px;align-items:center;margin:8px 0}
input,button{padding:6px 10px;font-size:14px}
ul{padding-left:20px}
.del{background:#f44336;color:white;border:none}
.save{background:#2196F3;color:white;border:none}
.add{background:#4CAF50;color:white;border:none}
#keyCapture{border:2px dashed #222;padding:24px;text-align:center;margin:12px 0;cursor:pointer}
.realtime{background:#f7f7f7;padding:12px;border-radius:6px;margin:8px 0}
.press{color:#2e7d32;font-weight:bold}
.release{color:#c62828;font-weight:bold}
.log-box{max-height:220px;overflow-y:auto;border:1px solid #ccc;padding:10px;margin-top:10px}
</style>
</head>
<body>
<h1>输入设备全局映射管理面板（WebSocket零延迟）</h1>

<div class="card">
<h3>1. API接口列表管理</h3>
<div id="apiList"></div>
<div class="row">
<input id="newApiInput" placeholder="http://127.0.0.1:8080/receive">
<button class="add" onclick="addApi()">新增接口</button>
</div>
</div>

<div class="card">
<h3>2. 全局键位映射（程序全局捕获生效）</h3>
<div id="keymapList"></div>
<div class="row">
<input id="actInput" placeholder="动作名: prev_page / next_page / brightness">
<input id="keyInput" placeholder="多按键逗号分隔 Up,Left">
<button class="add" onclick="saveKeymapItem()">保存映射</button>
</div>
<div id="keyCapture" tabindex="0">点击框内，按下键盘自动转换后端识别键名</div>
</div>

<div class="card">
<h3>3. ⚡ 实时输入状态（WebSocket推送）</h3>
<div class="realtime">
<div>键盘最新：<span id="kbLatest"></span></div>
<div>手柄最新：<span id="gpLatest"></span></div>
</div>
<h4>实时事件日志（自动滚动）</h4>
<div class="log-box" id="eventLog"></div>
</div>

<script>
let apiData = [];
let keymapData = {};
let ws;
const logBox = document.getElementById("eventLog");
const kbLatest = document.getElementById("kbLatest");
const gpLatest = document.getElementById("gpLatest");

// 初始化WebSocket
function initWS(){
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${window.location.host}/ws`;
    ws = new WebSocket(url);
    ws.onmessage = function(ev){
        const data = JSON.parse(ev.data);
        renderWsEvent(data);
    };
    ws.onclose = () => {
        appendLog("WebSocket断开，3秒重连...");
        setTimeout(initWS,3000);
    };
    ws.onerror = (e) => appendLog("WebSocket异常："+e);
}

// 渲染后端推送的键盘/手柄事件
function renderWsEvent(evt){
    let text = "";
    let cls = evt.is_release ? "release" : "press";
    if(evt.type === "Keyboard"){
        text = `键盘 ${evt.is_release ? "松开" : "按下"}: ${evt.key}`;
        kbLatest.innerHTML = `<span class="${cls}">${evt.key} ${evt.is_release?"↑":"↓"}</span>`;
    }else if(evt.type === "Gamepad"){
        text = `手柄 ${evt.is_release ? "松开" : "按下"}: ${evt.btn}`;
        gpLatest.innerHTML = `<span class="${cls}">${evt.btn} ${evt.is_release?"↑":"↓"}</span>`;
    }
    appendLog(`<span class="${cls}">${text}</span>`);
}

function appendLog(html){
    const now = new Date().toLocaleTimeString();
    logBox.innerHTML += `<div>[${now}] ${html}</div>`;
    logBox.scrollTop = logBox.scrollHeight;
    // 限制日志最多100条
    const items = logBox.querySelectorAll("div");
    if(items.length > 100) items[0].remove();
}

window.onload = function(){
    loadAll();
    initWS();
};

async function loadAll(){
    await loadApi();
    await loadKeymap();
}

// API管理
async function loadApi(){
    const res = await fetch("/api/config/api");
    const data = await res.json();
    apiData = data.api_list;
    renderApi(data);
}
function renderApi(cfg){
    const wrap = document.getElementById("apiList");
    let html = "";
    apiData.forEach((url,i)=>{
        const isDef = cfg.default_index === i;
        html += `<div class="row">
            <input value="${url}" id="api${i}" style="flex:1">
            <button onclick="setDefaultApi(${i})">${isDef?'默认':'设默认'}</button>
            <button class="del" onclick="delApi(${i})">删除</button>
        </div>`;
    })
    wrap.innerHTML = html;
}
async function addApi(){
    const val = document.getElementById("newApiInput").value.trim();
    if(!val) return alert("地址不能为空");
    apiData.push(val);
    await postApiCfg({api_list:apiData, default_idx:null});
    document.getElementById("newApiInput").value="";
    loadApi();
}
async function delApi(idx){
    apiData.splice(idx,1);
    await postApiCfg({api_list:apiData, default_idx:null});
    loadApi();
}
async function setDefaultApi(idx){
    await postApiCfg({api_list:apiData, default_idx:idx});
    loadAll();
}
async function postApiCfg(body){
    await fetch("/api/config/api",{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify(body)
    })
}

// 键位映射
async function loadKeymap(){
    const res = await fetch("/api/config/keymap");
    const data = await res.json();
    keymapData = data.map;
    renderKeymap();
}
function renderKeymap(){
    const wrap = document.getElementById("keymapList");
    let html = "";
    Object.entries(keymapData).forEach(([act,keys])=>{
        html += `<div class="row">
            <span style="width:120px">${act}:</span>
            <span style="flex:1">${Array.from(keys).join(',')}</span>
            <button onclick="editKeymap('${act}')">编辑</button>
            <button class="del" onclick="delKeymap('${act}')">删除</button>
        </div>`
    })
    wrap.innerHTML = html;
}
function editKeymap(act){
    document.getElementById("actInput").value = act;
    document.getElementById("keyInput").value = Array.from(keymapData[act]).join(',');
}
async function saveKeymapItem(){
    const act = document.getElementById("actInput").value.trim();
    const keyStr = document.getElementById("keyInput").value.trim();
    if(!act||!keyStr) return alert("动作/按键不能为空");
    const keys = keyStr.split(',').map(s=>s.trim()).filter(s=>s);
    await fetch("/api/config/keymap",{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({action:act,keys})
    })
    document.getElementById("actInput").value="";
    document.getElementById("keyInput").value="";
    loadKeymap();
}
async function delKeymap(act){
    await fetch("/api/config/keymap",{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({action:act,keys:[]})
    })
    loadKeymap();
}

// 前端捕获按键转换逻辑
const captureBox = document.getElementById("keyCapture");
captureBox.onkeydown = (e)=>{
    e.preventDefault();
    const rawWebKey = e.key;
    const converted = convertWebKeyToRdev(rawWebKey);
    const old = document.getElementById("keyInput").value;
    const arr = old.split(',').map(s=>s.trim()).filter(s=>s);
    if(!arr.includes(converted)) arr.push(converted);
    document.getElementById("keyInput").value = arr.join(',');
    captureBox.innerText = `捕获成功 | 原始浏览器键:${rawWebKey} → 后端识别:${converted}`;
}
function convertWebKeyToRdev(k){
    switch(k){
        case "ArrowUp": return "Up";
        case "ArrowDown": return "Down";
        case "ArrowLeft": return "Left";
        case "ArrowRight": return "Right";
        case " ": return "Space";
        case "Enter": return "Return";
        case "Backspace": return "Backspace";
        case "Escape": return "Esc";
        case "Tab": return "Tab";
        case "Shift": return "ShiftLeft";
        case "Control": return "ControlLeft";
        case "Alt": return "Alt";
        case "Meta": return "MetaLeft";
        case "0": return "Num0";
        case "1": return "Num1";
        case "2": return "Num2";
        case "3": return "Num3";
        case "4": return "Num4";
        case "5": return "Num5";
        case "6": return "Num6";
        case "7": return "Num7";
        case "8": return "Num8";
        case "9": return "Num9";
        case "-": return "Minus";
        case "=": return "Equal";
        case "[": return "LeftBracket";
        case "]": return "RightBracket";
        case ";": return "SemiColon";
        case "'": return "Quote";
        case "\\": return "BackSlash";
        case ",": return "Comma";
        case ".": return "Dot";
        case "/": return "Slash";
        case "`": return "BackQuote";
        default:
            if(k.length === 1 && /[a-z]/.test(k)) return "Key"+k.toUpperCase();
            return k;
    }
}
</script>
</body>
</html>
"#;
    Html(html.to_string())
}

// ========== Web接口处理器 ==========
async fn get_api_config(State(state): State<AppState>) -> Json<AppConfig> {
    Json(state.api_cfg.read().await.clone())
}
async fn update_api_config(State(state): State<AppState>, Json(req): Json<ApiEditReq>) -> StatusCode {
    let mut cfg = state.api_cfg.write().await;
    cfg.api_list = req.api_list;
    cfg.default_index = req.default_idx;
    let _ = save_api_config(&cfg);
    if let Some(idx) = cfg.default_index {
        if idx < cfg.api_list.len() {
            *state.target_api.write().await = Some(cfg.api_list[idx].clone());
        }
    } else {
        *state.target_api.write().await = None;
    }
    StatusCode::OK
}
async fn get_keymap(State(state): State<AppState>) -> Json<KeyMapConfig> {
    Json(state.keymap_cfg.read().await.clone())
}
async fn update_keymap(State(state): State<AppState>, Json(req): Json<KeymapEditReq>) -> StatusCode {
    let mut km = state.keymap_cfg.write().await;
    if req.keys.is_empty() {
        km.map.remove(&req.action);
    } else {
        let set: HashSet<String> = req.keys.into_iter().collect();
        km.map.insert(req.action, set);
    }
    let _ = save_keymap(&km);
    StatusCode::OK
}

// ========== Web服务启动 ==========
async fn start_web_server(state: AppState) {
    let router = Router::new()
        .route("/", get(index_page))
        // WebSocket长连接路由
        .route("/ws", get(ws_handler))
        .route("/api/config/api", get(get_api_config).post(update_api_config))
        .route("/api/config/keymap", get(get_keymap).post(update_keymap))
        .with_state(state);

    println!("🌐 Web配置面板: http://127.0.0.1:{}", WEB_PORT);
    println!("🔌 WebSocket实时推送地址: ws://127.0.0.1:{}/ws", WEB_PORT);
    let listener = TcpListener::bind(("127.0.0.1", WEB_PORT)).await.unwrap();
    axum::serve(listener, router.into_make_service()).await.unwrap();
}

// ========== 命令行选择API ==========
fn cli_select_api(config: &mut AppConfig) -> Option<String> {
    if config.api_list.is_empty() {
        println!("⚠️ 无保存接口，请打开网页添加：http://127.0.0.1:{}", WEB_PORT);
        return None;
    }
    if let Some(def_idx) = config.default_index {
        let url = config.api_list[def_idx].clone();
        println!("✅ 自动加载默认接口: {}", url);
        return Some(url);
    }
    loop {
        println!("\n===== 接口列表 =====");
        for (i, u) in config.api_list.iter().enumerate() {
            println!("{}. {}", i + 1, u);
        }
        println!("输入数字选择接口：");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();
        match input.parse::<usize>() {
            Ok(n) if n >= 1 && n <= config.api_list.len() => {
                return Some(config.api_list[n - 1].clone());
            }
            _ => println!("❌ 无效序号"),
        }
    }
}

// ========== 程序入口 ==========
fn main() -> std::io::Result<()> {
    setup_exit_handler();
    let args: Vec<String> = std::env::args().collect::<Vec<String>>();
    let mut config = load_api_config();
    let _is_setting_mode = args.iter().any(|arg| arg == "-s");

    // 全局复用Tokio Runtime
    let rt = Arc::new(Runtime::new()?);
    let selected_api = cli_select_api(&mut config);

    // WebSocket客户端连接池初始化
    let ws_clients = Arc::new(RwLock::new(Vec::new()));

    let state = AppState {
        api_cfg: Arc::new(RwLock::new(config.clone())),
        keymap_cfg: Arc::new(RwLock::new(load_keymap())),
        target_api: Arc::new(RwLock::new(selected_api)),
        rt: rt.clone(),
        ws_clients,
    };

    // 启动键鼠、手柄监听线程
    start_keyboard_listener(state.clone());
    start_gamepad_listener(state.clone());

    // 阻塞运行Web服务
    rt.block_on(start_web_server(state));
    Ok(())
}