//! `kharka service`: a launchd agent (macOS) or a systemd user unit (Linux) that keeps a kharka role running.

use std::fmt::Write;
use std::path::Path;

/// What the service file is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum System {
    Launchd,
    Systemd,
}

impl System {
    pub fn here() -> System {
        if cfg!(target_os = "macos") {
            System::Launchd
        } else {
            System::Systemd
        }
    }
}

/// The service file that runs `<exe> <role> <args…>`, restarted whenever it ends, its log in `log`.
pub fn file(system: System, exe: &Path, role: &str, args: &[String], log: &Path) -> String {
    let mut command = vec![exe.display().to_string(), role.to_string()];
    command.extend(args.iter().cloned());
    match system {
        System::Launchd => launchd(role, &command, log),
        System::Systemd => systemd(role, &command),
    }
}

/// Where the service file goes, and how it is started.
pub fn instructions(system: System, role: &str) -> String {
    match system {
        System::Launchd => format!(
            "kharka service {role} … > ~/Library/LaunchAgents/{label}.plist\n\
             launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/{label}.plist",
            label = label(role)
        ),
        System::Systemd => format!(
            "kharka service {role} … > ~/.config/systemd/user/kharka-{role}.service\n\
             systemctl --user daemon-reload && systemctl --user enable --now kharka-{role}\n\
             loginctl enable-linger \"$USER\"   # keeps it running without a login (a server)"
        ),
    }
}

pub fn label(role: &str) -> String {
    format!("dev.kharka.{role}")
}

fn launchd(role: &str, command: &[String], log: &Path) -> String {
    let mut arguments = String::new();
    for part in command {
        let _ = writeln!(arguments, "        <string>{}</string>", xml(part));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
{arguments}    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>5</integer>
    <key>StandardErrorPath</key>
    <string>{log}</string>
    <key>StandardOutPath</key>
    <string>{log}</string>
</dict>
</plist>
"#,
        label = label(role),
        log = xml(&log.display().to_string()),
    )
}

fn systemd(role: &str, command: &[String]) -> String {
    let exec: Vec<String> = command.iter().map(|part| systemd_quote(part)).collect();
    format!(
        "[Unit]\n\
         Description=kharka {role}\n\
         Wants=network-online.target\n\
         After=network-online.target\n\
         \n\
         [Service]\n\
         ExecStart={}\n\
         Restart=always\n\
         RestartSec=2\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exec.join(" ")
    )
}

fn xml(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn systemd_quote(part: &str) -> String {
    if !part.is_empty()
        && part
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-:=@+,".contains(&b))
    {
        return part.to_string();
    }
    format!(
        "\"{}\"",
        part.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_launchd_agent() {
        let args = vec!["--hub".to_string(), "server".to_string()];
        let plist = file(
            System::Launchd,
            Path::new("/opt/homebrew/bin/kharka"),
            "daemon",
            &args,
            Path::new("/x/kharka.log"),
        );
        assert!(plist.contains("<string>dev.kharka.daemon</string>"));
        assert!(plist.contains("<string>/opt/homebrew/bin/kharka</string>\n        <string>daemon</string>\n        <string>--hub</string>\n        <string>server</string>\n    </array>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
    }

    #[test]
    fn a_systemd_unit() {
        let args = vec!["--node".to_string(), "two words".to_string()];
        let unit = file(
            System::Systemd,
            Path::new("/home/me/.cargo/bin/kharka"),
            "serve",
            &args,
            Path::new("-"),
        );
        assert!(unit.contains("ExecStart=/home/me/.cargo/bin/kharka serve --node \"two words\"\n"));
        assert!(unit.contains("Restart=always"));
    }
}
