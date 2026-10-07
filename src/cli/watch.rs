//! `dkv watch`: the cluster as a live picture instead of a scrolling log.
//!
//! No TUI crate. The whole trick is two escape codes: `\x1b[H` moves the cursor
//! home, `\x1b[K` clears to end of line. Redrawing every line over the old one
//! and never clearing the screen is what stops it flickering.

use super::commands::{Cluster, fmt_hash};
use crate::proto::NodeStatus;
use crate::types::NodeId;
use std::time::Duration;

const HOME: &str = "\x1b[H";
const CLEAR_LINE: &str = "\x1b[K";
const HIDE_CURSOR: &str = "\x1b[?25l";
const SHOW_CURSOR: &str = "\x1b[?25h";
const COL: usize = 30;

pub async fn run(cluster: Cluster, fps: u32, narrow: bool) {
    let interval = Duration::from_millis(1000 / fps.clamp(1, 20) as u64);
    print!("\x1b[2J{HIDE_CURSOR}");
    let mut frames: u64 = 0;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        let statuses = cluster.status_all().await;
        frames += 1;
        let frame = render(&cluster, &statuses, narrow, frames);
        print!("{HOME}{frame}");
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    println!("{SHOW_CURSOR}");
}

fn render(
    cluster: &Cluster,
    statuses: &[(NodeId, Option<NodeStatus>)],
    narrow: bool,
    frames: u64,
) -> String {
    let mut out = String::new();
    let mut line = |s: String| {
        out.push_str(&s);
        out.push_str(CLEAR_LINE);
        out.push('\n');
    };

    line(cluster.bold(&format!(
        "dkv watch   {}   frame {frames}   (Ctrl-C to quit)",
        crate::logfmt::timestamp()
    )));
    line(String::new());

    let panels: Vec<Vec<String>> = statuses
        .iter()
        .map(|(id, s)| panel(cluster, *id, s.as_ref()))
        .collect();

    if narrow {
        for p in &panels {
            for row in p {
                line(row.clone());
            }
            line(String::new());
        }
    } else {
        let rows = panels.iter().map(|p| p.len()).max().unwrap_or(0);
        for r in 0..rows {
            let mut composed = String::new();
            for p in &panels {
                let cell = p.get(r).cloned().unwrap_or_default();
                composed.push_str(&pad_visible(&cell, COL));
                composed.push_str("  ");
            }
            line(composed);
        }
    }

    line(String::new());
    line(cluster.dim("peer view (row = who is looking, col = at whom)"));
    line(format!(
        "       {}",
        statuses
            .iter()
            .map(|(id, _)| format!("  n{id} "))
            .collect::<String>()
    ));
    for (id, s) in statuses {
        let mut row = format!("  n{id}   ");
        for (other, _) in statuses {
            if other == id {
                row.push_str("  -  ");
                continue;
            }
            let cell = match s {
                None => cluster.dim(" ?? "),
                Some(st) => match st.peers.iter().find(|p| p.id == *other) {
                    Some(p) if p.reachable => cluster.good(" ok "),
                    Some(_) => cluster.bad(" xx "),
                    None => cluster.dim(" -- "),
                },
            };
            row.push_str(&cell);
            row.push(' ');
        }
        line(row);
    }

    line(String::new());
    let mut faults: Vec<String> = Vec::new();
    for (id, s) in statuses {
        if let Some(st) = s {
            for f in &st.faults {
                if f.partitioned {
                    faults.push(format!("n{id} -x- n{}", f.peer));
                } else {
                    faults.push(format!(
                        "n{id} -> n{} drop {:.0}% +{}ms",
                        f.peer,
                        f.drop_rate * 100.0,
                        f.delay_ms
                    ));
                }
            }
        }
    }
    if faults.is_empty() {
        line(cluster.dim("faults: none"));
    } else {
        line(cluster.warn(&format!("faults: {}", faults.join("   "))));
    }

    let up: Vec<&NodeStatus> = statuses.iter().filter_map(|(_, s)| s.as_ref()).collect();
    let converged = up.len() == statuses.len()
        && up
            .iter()
            .all(|s| s.state_hash == up[0].state_hash && s.last_applied == up[0].last_applied);
    let verdict = if converged {
        cluster.good("state: CONVERGED — every node has the same applied index and hash")
    } else if up.len() < statuses.len() {
        cluster.warn(&format!(
            "state: {} of {} nodes answering",
            up.len(),
            statuses.len()
        ))
    } else {
        cluster.warn("state: nodes differ (catching up, or diverged — run `dkv verify`)")
    };
    line(verdict);
    // A few blank lines so a previously taller frame leaves no residue.
    for _ in 0..3 {
        line(String::new());
    }
    out
}

fn panel(cluster: &Cluster, id: NodeId, status: Option<&NodeStatus>) -> Vec<String> {
    let Some(s) = status else {
        return vec![
            format!("{} {}", cluster.tag(id), cluster.bad("DOWN")),
            cluster.dim("  no answer"),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ];
    };
    let role = match s.role.as_str() {
        "LEADER" => cluster.good("LEADER"),
        "CANDIDATE" => cluster.warn("CANDIDATE"),
        other => other.to_string(),
    };
    vec![
        format!("{} {}", cluster.tag(id), role),
        format!("  term     {:>6}", s.term),
        format!("  commit   {:>6}", s.commit_index),
        format!("  applied  {:>6}", s.last_applied),
        format!("  log   {:>5}..{:<5}", s.first_index, s.last_index),
        format!("  keys     {:>6}", s.keys),
        format!("  hash   {}", fmt_hash(s.state_hash)),
        format!(
            "  leader   {:>6}",
            s.leader
                .map(|l| format!("n{l}"))
                .unwrap_or_else(|| "-".into())
        ),
    ]
}

/// Pads to a visible width, ignoring the ANSI escape bytes that would
/// otherwise make coloured cells come out narrower than plain ones.
fn pad_visible(s: &str, width: usize) -> String {
    let visible = visible_len(s);
    if visible >= width {
        return s.to_string();
    }
    format!("{s}{}", " ".repeat(width - visible))
}

fn visible_len(s: &str) -> usize {
    let mut n = 0;
    let mut in_escape = false;
    for ch in s.chars() {
        if in_escape {
            if ch == 'm' {
                in_escape = false;
            }
            continue;
        }
        if ch == '\x1b' {
            in_escape = true;
            continue;
        }
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colour_escapes_do_not_count_toward_column_width() {
        assert_eq!(visible_len("\x1b[32mok\x1b[0m"), 2);
        assert_eq!(
            pad_visible("\x1b[32mok\x1b[0m", 5).len(),
            "\x1b[32mok\x1b[0m".len() + 3
        );
    }

    #[test]
    fn a_cell_already_wider_than_the_column_is_left_alone() {
        assert_eq!(pad_visible("abcdefgh", 4), "abcdefgh");
    }
}
