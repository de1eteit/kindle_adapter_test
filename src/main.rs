use gilrs::{Button, Event, Gilrs};
use once_cell::sync::Lazy;
use reqwest::blocking::Client;
use std::collections::{HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;

#[cfg(unix)]
pub use {
    signal_hook::consts::SIGINT,
    signal_hook::iterator::Signals,
};
//#[cfg(windows)]
use ctrlc;

#[cfg(all(feature = "other", not(feature = "linux")))]
use rdev::{grab, Event as rEvent, EventType, Key};

#[cfg(all(target_os = "linux", feature = "other"))]
use {
    crossbeam::channel::unbounded,
    evdev::{Device, KeyCode, EventType as EvEventType},
    libudev::Context,
};

// 节流间隔 200ms
const THROTTLE_MS: u64 = 200;
// 键盘轮询间隔，降低CPU
const KEY_POLL_SLEEP: Duration = Duration::from_millis(30);

#[derive(Debug, Default)]
struct AppConfig {
    api_list: Vec<String>,
    default_index: Option<usize>,
}

// ================== 按键名称映射函数 rdev macOS Windows ==================
#[cfg(all(feature = "other", not(feature = "linux")))]
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
        Key::KeyQ => "Q".to_string(),
        Key::KeyW => "W".to_string(),
        Key::KeyE => "E".to_string(),
        Key::KeyR => "R".to_string(),
        Key::KeyT => "T".to_string(),
        Key::KeyY => "Y".to_string(),
        Key::KeyU => "U".to_string(),
        Key::KeyI => "I".to_string(),
        Key::KeyO => "O".to_string(),
        Key::KeyP => "P".to_string(),
        Key::LeftBracket => "LeftBracket".to_string(),
        Key::RightBracket => "RightBracket".to_string(),
        Key::KeyA => "A".to_string(),
        Key::KeyS => "S".to_string(),
        Key::KeyD => "D".to_string(),
        Key::KeyF => "F".to_string(),
        Key::KeyG => "G".to_string(),
        Key::KeyH => "H".to_string(),
        Key::KeyJ => "J".to_string(),
        Key::KeyK => "K".to_string(),
        Key::KeyL => "L".to_string(),
        Key::SemiColon => "SemiColon".to_string(),
        Key::Quote => "Quote".to_string(),
        Key::BackSlash => "BackSlash".to_string(),
        Key::IntlBackslash => "IntlBackslash".to_string(),
        Key::KeyZ => "Z".to_string(),
        Key::KeyX => "X".to_string(),
        Key::KeyC => "C".to_string(),
        Key::KeyV => "V".to_string(),
        Key::KeyB => "B".to_string(),
        Key::KeyN => "N".to_string(),
        Key::KeyM => "M".to_string(),
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

// Linux evdev KeyCode -> 统一上报名称
#[cfg(all(target_os = "linux", feature = "linux"))]
fn evdev_key_to_name(code: KeyCode) -> Option<String> {
    let name = match code {
        KeyCode::KEY_ESC => "Esc",
        KeyCode::KEY_1 => "Num1",
        KeyCode::KEY_2 => "Num2",
        KeyCode::KEY_3 => "Num3",
        KeyCode::KEY_4 => "Num4",
        KeyCode::KEY_5 => "Num5",
        KeyCode::KEY_6 => "Num6",
        KeyCode::KEY_7 => "Num7",
        KeyCode::KEY_8 => "Num8",
        KeyCode::KEY_9 => "Num9",
        KeyCode::KEY_0 => "Num0",
        KeyCode::KEY_MINUS => "Minus",
        KeyCode::KEY_EQUAL => "Equal",
        KeyCode::KEY_BACKSPACE => "Backspace",
        KeyCode::KEY_TAB => "Tab",
        KeyCode::KEY_Q => "Q",
        KeyCode::KEY_W => "W",
        KeyCode::KEY_E => "E",
        KeyCode::KEY_R => "R",
        KeyCode::KEY_T => "T",
        KeyCode::KEY_Y => "Y",
        KeyCode::KEY_U => "U",
        KeyCode::KEY_I => "I",
        KeyCode::KEY_O => "O",
        KeyCode::KEY_P => "P",
        KeyCode::KEY_LEFTBRACE => "LeftBracket",
        KeyCode::KEY_RIGHTBRACE => "RightBracket",
        KeyCode::KEY_ENTER => "Return",
        KeyCode::KEY_LEFTCTRL => "ControlLeft",
        KeyCode::KEY_A => "A",
        KeyCode::KEY_S => "S",
        KeyCode::KEY_D => "D",
        KeyCode::KEY_F => "F",
        KeyCode::KEY_G => "G",
        KeyCode::KEY_H => "H",
        KeyCode::KEY_J => "J",
        KeyCode::KEY_K => "K",
        KeyCode::KEY_L => "L",
        KeyCode::KEY_SEMICOLON => "SemiColon",
        KeyCode::KEY_APOSTROPHE => "Quote",
        KeyCode::KEY_GRAVE => "BackQuote",
        KeyCode::KEY_LEFTSHIFT => "ShiftLeft",
        KeyCode::KEY_BACKSLASH => "BackSlash",
        KeyCode::KEY_Z => "Z",
        KeyCode::KEY_X => "X",
        KeyCode::KEY_C => "C",
        KeyCode::KEY_V => "V",
        KeyCode::KEY_B => "B",
        KeyCode::KEY_N => "N",
        KeyCode::KEY_M => "M",
        KeyCode::KEY_COMMA => "Comma",
        KeyCode::KEY_DOT => "Dot",
        KeyCode::KEY_SLASH => "Slash",
        KeyCode::KEY_RIGHTSHIFT => "ShiftRight",
        KeyCode::KEY_KPASTERISK => "KpMultiply",
        KeyCode::KEY_LEFTALT => "Alt",
        KeyCode::KEY_SPACE => "Space",
        KeyCode::KEY_CAPSLOCK => "CapsLock",
        KeyCode::KEY_F1 => "F1",
        KeyCode::KEY_F2 => "F2",
        KeyCode::KEY_F3 => "F3",
        KeyCode::KEY_F4 => "F4",
        KeyCode::KEY_F5 => "F5",
        KeyCode::KEY_F6 => "F6",
        KeyCode::KEY_F7 => "F7",
        KeyCode::KEY_F8 => "F8",
        KeyCode::KEY_F9 => "F9",
        KeyCode::KEY_F10 => "F10",
        KeyCode::KEY_NUMLOCK => "NumLock",
        KeyCode::KEY_SCROLLLOCK => "ScrollLock",
        KeyCode::KEY_KP7 => "Kp7",
        KeyCode::KEY_KP8 => "Kp8",
        KeyCode::KEY_KP9 => "Kp9",
        KeyCode::KEY_KPMINUS => "KpMinus",
        KeyCode::KEY_KP4 => "Kp4",
        KeyCode::KEY_KP5 => "Kp5",
        KeyCode::KEY_KP6 => "Kp6",
        KeyCode::KEY_KPPLUS => "KpPlus",
        KeyCode::KEY_KP1 => "Kp1",
        KeyCode::KEY_KP2 => "Kp2",
        KeyCode::KEY_KP3 => "Kp3",
        KeyCode::KEY_KP0 => "Kp0",
        KeyCode::KEY_KPDOT => "KpDelete",
        KeyCode::KEY_F11 => "F11",
        KeyCode::KEY_F12 => "F12",
        KeyCode::KEY_KPENTER => "KpReturn",
        KeyCode::KEY_RIGHTCTRL => "ControlRight",
        KeyCode::KEY_KPSLASH => "KpDivide",
        KeyCode::KEY_SYSRQ => "PrintScreen",
        KeyCode::KEY_RIGHTALT => "AltGr",
        KeyCode::KEY_HOME => "Home",
        KeyCode::KEY_UP => "Up",
        KeyCode::KEY_PAGEUP => "PageUp",
        KeyCode::KEY_LEFT => "Left",
        KeyCode::KEY_RIGHT => "Right",
        KeyCode::KEY_END => "End",
        KeyCode::KEY_DOWN => "Down",
        KeyCode::KEY_PAGEDOWN => "PageDown",
        KeyCode::KEY_INSERT => "Insert",
        KeyCode::KEY_DELETE => "Delete",
        KeyCode::KEY_PAUSE => "Pause",
        KeyCode::KEY_LEFTMETA => "MetaLeft",
        KeyCode::KEY_RIGHTMETA => "MetaRight",
        _ => return None,
    };
    Some(name.to_string())
}

// ==================配置文件相关==================
fn get_config_path() -> PathBuf {
    let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    let dir = exe_path.parent().unwrap_or_else(|| Path::new("."));
    dir.join("config.txt")
}

fn load_config() -> AppConfig {
    let path = get_config_path();
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

    AppConfig {
        api_list,
        default_index,
    }
}

fn save_config(cfg: &AppConfig) -> std::io::Result<()> {
    let path = get_config_path();
    let mut content = String::with_capacity(256);
    let idx = cfg.default_index.unwrap_or(9999);
    content.push_str(&format!("{}\n", idx));
    for url in &cfg.api_list {
        content.push_str(url);
        content.push('\n');
    }
    std::fs::write(path, content)
}

fn read_input() -> String {
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap_or_default();
    input.trim().to_lowercase()
}

fn ask_set_default(cfg: &mut AppConfig, current_url: &str) -> std::io::Result<()> {
    println!("\n是否将该接口设为【下次默认启动接口】？(y/n)");
    let ans = read_input();
    if ans == "y" || ans == "yes" {
        if let Some(idx) = cfg.api_list.iter().position(|u| u == current_url) {
            cfg.default_index = Some(idx);
            save_config(cfg)?;
            println!("✅ 已设置为默认接口，下次启动自动选用");
        }
    } else {
        println!("ℹ️ 不设置默认接口");
    }
    Ok(())
}

fn input_new_url(cfg: &mut AppConfig) -> std::io::Result<String> {
    println!("请输入HTTP接口地址(例如 http://127.0.0.1:8080/receive)：");
    loop {
        let url = read_input();
        if url.is_empty() {
            eprintln!("❌ 地址不能为空，请重新输入");
            continue;
        }
        cfg.api_list.push(url.clone());
        save_config(cfg)?;
        println!("✅ 已保存新接口");
        return Ok(url);
    }
}

fn set_default_interface(cfg: &mut AppConfig) -> std::io::Result<()> {
    if cfg.api_list.is_empty() {
        eprintln!("❌ 暂无接口可设置");
        return Ok(());
    }
    println!("\n===== 选择要设为默认的接口 =====");
    for (idx, url) in cfg.api_list.iter().enumerate() {
        let mark = if cfg.default_index == Some(idx) { "[当前默认] " } else { "" };
        println!("{} {}. {}", mark, idx + 1, url);
    }
    println!("请输入接口序号：");

    loop {
        let input = read_input();
        match input.parse::<usize>() {
            Ok(n) => {
                let real_idx = n - 1;
                if real_idx < cfg.api_list.len() {
                    cfg.default_index = Some(real_idx);
                    save_config(cfg)?;
                    println!("✅ 成功将第 {} 个接口设为默认", n);
                    return Ok(());
                } else {
                    eprintln!("❌ 序号超出范围，请重新输入");
                }
            }
            Err(_) => eprintln!("❌ 请输入合法数字"),
        }
    }
}

fn clear_default(cfg: &mut AppConfig) -> std::io::Result<()> {
    cfg.default_index = None;
    save_config(cfg)?;
    println!("✅ 已清空默认接口配置");
    Ok(())
}

fn setting_mode(mut config: AppConfig) -> std::io::Result<()> {
    println!("========== 配置设置模式 ==========");
    println!("1. 新增接口\n2. 设置默认接口\n3. 清空默认接口\n4. 退出设置\n");
    loop {
        println!("\n===== 已保存接口列表 =====");
        for (index, url) in config.api_list.iter().enumerate() {
            let mark = if config.default_index == Some(index) { "[默认]" } else { "" };
            println!("{}{}. {}", mark, index + 1, url);
        }
        println!("==========================");
        println!("请选择功能序号：1新增 2设默认 3清空默认 4退出");
        let input = read_input();
        match input.as_str() {
            "1" => {
                let _ = input_new_url(&mut config)?;
            }
            "2" => set_default_interface(&mut config)?,
            "3" => clear_default(&mut config)?,
            "4" => {
                println!("👋 退出设置模式");
                break;
            }
            _ => eprintln!("❌ 无效选项，请输入 1/2/3/4"),
        }
    }
    Ok(())
}

// ==================网络请求==================
static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .timeout(Duration::from_secs(3))
        .pool_max_idle_per_host(4)
        .build()
        .unwrap()
});

fn spawn_send_key_by_get(url: &Arc<String>, key_name: String) {
    let full_url = format!("{}/api/keycode?keyname={}", url, key_name);
    if let Err(e) = HTTP_CLIENT.get(&full_url).send() {
        eprintln!("[网络] key发送失败: {}", e);
    }
}

fn spawn_send_op(url: Arc<String>, op: &str) {
    let full_url = format!("{}/op/{}", url, op);
    if let Err(e) = HTTP_CLIENT.get(&full_url).send() {
        eprintln!("[网络] key发送失败: {}", e);
    }
}

// ================== 退出信号处理 ==================
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

use std::sync::atomic::{AtomicBool, Ordering};

fn setup_signal_handler() -> Arc<AtomicBool> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    // ctrlc::set_handler(move || {
    //     if r.swap(false, Ordering::SeqCst) {
    //         println!("🛑 收到退出信号，准备关闭...");
    //     }
    // }).expect("设置 Ctrl+C 失败");
    ctrlc::set_handler(|| {
        eprintln!("\n🛑 Ctrl+C 退出");
        std::process::exit(0);
    }).unwrap();

    running
}

// ================== Linux专用：枚举所有输入设备 ==================
#[cfg(all(target_os = "linux", feature = "linux"))]
fn enumerate_input_devices() -> Vec<PathBuf> {
    let ctx = Context::new().unwrap();
    let mut enumerator = libudev::Enumerator::new(&ctx).unwrap();
    enumerator.match_subsystem("input").unwrap();
    let mut paths = Vec::new();

    for device in enumerator.scan_devices().unwrap() {
        if let Some(dev_node) = device.devnode() {
            let path = PathBuf::from(dev_node);
            if path.to_str().map(|s| s.starts_with("/dev/input/event")).unwrap_or(false) {
                paths.push(path);
            }
        }
    }
    paths
}

// Linux键鼠监听线程
#[cfg(all(target_os = "linux", feature = "linux"))]
fn spawn_evdev_listener(target_url: Arc<String>) {
    let dev_paths = enumerate_input_devices();
    if dev_paths.is_empty() {
        eprintln!("❌ 未找到任何 /dev/input/event* 设备，请确认接入键盘鼠标并使用root(或加入input组)运行程序");
        std::process::exit(1);
    }

    println!("📥 检测到输入设备：{:?}", dev_paths);
    //let (tx, rx) = mpsc::channel::<(String, String)>();
    //let (tx, rx): (Sender<(String, String)>, Receiver<(String, String)>) = unbounded();
    let (tx, rx) = unbounded::<(String, String)>();

    // 每个设备单独开线程监听
    for dev_path in dev_paths {
        let tx_clone = tx.clone();

        thread::spawn(move || {
            let mut dev = match Device::open(&dev_path) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("⚠️ 打开设备 {:?} 失败: {} (需要root(或input组)权限)", dev_path, e);
                    return;
                }
            };
            println!("✅ 开始监听设备 {:?}", dev_path);

            loop {
                match dev.fetch_events() {
                    Ok(events) => {
                        for ev in events {
                            match ev.event_type() {
                                EvEventType::KEY => {
                                    let code = KeyCode::new(ev.code());
                                    let val = ev.value();
                                    // val=0 松开；1按下；2长按重复
                                    if val == 0 {
                                        let send_res = match code {
                                            KeyCode::BTN_LEFT => tx_clone.send(("mouse".into(), "mouse_Left".into())).ok(),
                                            KeyCode::BTN_RIGHT => tx_clone.send(("mouse".into(), "mouse_Right".into())).ok(),
                                            KeyCode::BTN_MIDDLE => tx_clone.send(("mouse".into(), "mouse_Middle".into())).ok(),
                                            _ => {
                                                if let Some(name) = evdev_key_to_name(code) {
                                                    let _ = tx_clone.send(("key".into(), name)).ok();
                                                }
                                                None
                                            }
                                        };
                                        let _ = send_res;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("设备 {:?} 读取中断: {}", dev_path, e);
                        break;
                    }
                }
            }
        });
    }

    // 主线程消费按键事件，统一上报
    // thread::spawn(move || {
    //     while let Ok((ty, name)) = rx.recv() {
    //         match ty.as_str() {
    //             "key" => println!("[按键松开] {}", name),
    //             "mouse" => println!("[鼠标松开] {}", name),
    //             _ => {}
    //         }
    //         spawn_send_key_by_get(&target_url, name);
    //     }
    // });
    thread::spawn(move || {
        while let Ok((ty, name)) = rx.recv() {
            match ty.as_str() {
                "key" => {
                    println!("[按键松开] {}", name);
                    spawn_send_key_by_get(&target_url, name);
                }
                "mouse" => {
                    println!("[鼠标松开] {}", name);
                    spawn_send_key_by_get(&target_url, name);
                }
                _ => {}
            }
        }
    });

}


// ================== 主函数 ==================
fn main() -> std::io::Result<()> {
    setup_exit_handler();
    //let running = setup_signal_handler();

    #[cfg(all(target_os = "linux", feature = "linux"))]
    println!("🔹 当前运行模式：Linux Server/X/Wayland evdev输入监听");

    #[cfg(feature = "other")]
    println!("🔹 当前运行模式：macOS/Windows输入监听(rdev)");

    let args: Vec<String> = std::env::args().collect();
    let is_setting_mode = args.iter().any(|arg| arg == "-s");
    let mut config = load_config();
    let cfg_path = get_config_path();
    println!("配置文件路径: {:?}\n", cfg_path);

    if is_setting_mode {
        return setting_mode(config);
    }

    println!("===========  键盘鼠标手柄监听翻页工具 ===========");
    println!("删除 config.txt 清空所有接口；启动加 -s 进入配置模式\n");

    let select_url: Arc<String> = if config.api_list.is_empty() {
        println!("当前暂无已保存接口，请输入新接口地址");
        let url = input_new_url(&mut config)?;
        ask_set_default(&mut config, &url)?;
        Arc::new(url)
    } else {
        if let Some(def_idx) = config.default_index {
            let url = Arc::new(config.api_list[def_idx].clone());
            println!("✅ 使用默认接口: {}\n", url);
            url
        } else {
            loop {
                println!("\n===== 已保存接口 =====");
                for (idx, url) in config.api_list.iter().enumerate() {
                    let mark = if config.default_index == Some(idx) { "[默认]" } else { "" };
                    println!("{}{}. {}", mark, idx + 1, url);
                }
                println!("======================");
                println!("数字=选中接口启动 | 0=新增 | m=设置默认 | q=清空默认");
                let input = read_input();
                match input.as_str() {
                    "m" => {
                        set_default_interface(&mut config)?;
                        continue;
                    }
                    "q" => {
                        clear_default(&mut config)?;
                        continue;
                    }
                    "0" => {
                        let new_url = input_new_url(&mut config)?;
                        ask_set_default(&mut config, &new_url)?;
                        continue;
                    }
                    _ => match input.parse::<usize>() {
                        Ok(n) => {
                            let real_idx = n - 1;
                            if real_idx < config.api_list.len() {
                                let url = Arc::new(config.api_list[real_idx].clone());
                                println!("✅ 选中接口: {}", url);
                                break url;
                            } else {
                                eprintln!("❌ 序号超出范围");
                            }
                        }
                        Err(_) => eprintln!("❌ 输入无效"),
                    },
                }
            }
        }
    };

    println!("\n---------- 监听已启动 (Ctrl+C 退出) ----------");

    // 图形桌面 Windows/macOS/X11/Wayland rdev监听
    #[cfg(all(feature = "other", not(feature = "linux")))]
    {
        let url_clone = select_url.clone();
        let callback = move |event: rEvent| -> Option<rEvent> {
            match &event.event_type {
                EventType::KeyRelease(key) => {
                    println!("[按键松开] {:?}", key);
                    let key_name = process_key(key);
                    spawn_send_key_by_get(&url_clone, key_name);
                }
                EventType::ButtonRelease(btn) => {
                    println!("[鼠标松开] {:?}", btn);
                    let mouse = format!("{:?}", btn);
                    if mouse == "Left" {
                        spawn_send_key_by_get(&url_clone, "mouse_Left".to_string());
                    } else if mouse == "Right" {
                        spawn_send_key_by_get(&url_clone, "mouse_Right".to_string());
                    }
                }
                _ => {}
            }
            Some(event)
        };

        let _listener_handle = thread::spawn(move || {
            grab(callback).expect("grab启动失败，请检查图形环境/权限");
        });
    }

    // Linux evdev键鼠监听
    #[cfg(all(target_os = "linux", feature = "linux"))]
    spawn_evdev_listener(select_url.clone());

    // ========== 游戏手柄监听 ==========
    let pad_url = Arc::clone(&select_url);
    let mut gilrs = Gilrs::new().expect("初始化手柄失败");
    let mut pad_throttle = HashMap::<Button, Instant>::new();


    // 主循环（处理手柄）
    loop {
        while let Some(Event { event, .. }) = gilrs.next_event() {
            match event {
                gilrs::EventType::ButtonReleased(Button::South, _) => {
                    let now = Instant::now();
                    let last = pad_throttle.entry(Button::South).or_insert(now);
                    if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                        if now.duration_since(*last).as_millis() != 0 {
                            continue;
                        }
                    }
                    *last = now;
                    spawn_send_key_by_get(&pad_url.clone(),"South".to_string());
                },
                gilrs::EventType::ButtonReleased(Button::East, _) => {
                    let now = Instant::now();
                    let last = pad_throttle.entry(Button::East).or_insert(now);
                    if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                        if now.duration_since(*last).as_millis() != 0 {
                            continue;
                        }
                    }
                    *last = now;
                    spawn_send_key_by_get(&pad_url.clone(),"East".to_string());
                },
                gilrs::EventType::ButtonReleased(Button::North, _) => {
                    let now = Instant::now();
                    let last = pad_throttle.entry(Button::North).or_insert(now);
                    if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                        if now.duration_since(*last).as_millis() != 0 {
                            continue;
                        }
                    }
                    *last = now;
                    spawn_send_key_by_get(&pad_url.clone(),"North".to_string());
                },
                gilrs::EventType::ButtonReleased(Button::West, _) => {
                    let now = Instant::now();
                    let last = pad_throttle.entry(Button::West).or_insert(now);
                    if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                        if now.duration_since(*last).as_millis() != 0 {
                            continue;
                        }
                    }
                    *last = now;
                    spawn_send_key_by_get(&pad_url.clone(),"West".to_string());
                },
                gilrs::EventType::ButtonReleased(Button::DPadUp, _) => {
                    let now = Instant::now();
                    let last = pad_throttle.entry(Button::DPadUp).or_insert(now);
                    if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                        if now.duration_since(*last).as_millis() != 0 {
                            continue;
                        }
                    }
                    *last = now;
                    spawn_send_key_by_get(&pad_url.clone(),"DPadUp".to_string());
                },
                gilrs::EventType::ButtonReleased(Button::DPadDown, _) => {
                    let now = Instant::now();
                    let last = pad_throttle.entry(Button::DPadDown).or_insert(now);
                    if now.duration_since(*last).as_millis() < THROTTLE_MS as u128 {
                        if now.duration_since(*last).as_millis() != 0 {
                            continue;
                        }
                    }
                    *last = now;
                    spawn_send_key_by_get(&pad_url.clone(),"DPadDown".to_string());
                },
                _ => {}
            }
        }
        thread::sleep(KEY_POLL_SLEEP);
    }
}