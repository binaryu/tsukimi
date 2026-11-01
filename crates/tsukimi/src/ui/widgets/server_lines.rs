//! Server line picker.
//!
//! A server may be reachable through several addresses (LAN, WAN, relay). The
//! client keeps using whichever one works and falls back automatically, but the
//! user needs a way to see the list, measure it and pin a line. This dialog is
//! that surface, built in code so it needs no template changes.

use std::{
    cell::RefCell,
    rc::Rc,
};

use adw::prelude::*;
use gettextrs::gettext;

use crate::{
    client::jellyfin_client::{
        JELLYFIN_CLIENT,
        LineStatus,
    },
    utils::{
        spawn,
        spawn_tokio,
    },
};

/// Latency labels of the currently built rows, keyed by line URL.
type LatencyLabels = Rc<RefCell<Vec<(String, gtk::Label)>>>;

pub fn present(parent: &impl IsA<gtk::Widget>) {
    let dialog = adw::Dialog::builder()
        .title(gettext("Server Lines"))
        .content_width(560)
        .content_height(520)
        .build();

    let overlay = adw::ToastOverlay::new();
    let toolbar = adw::ToolbarView::new();
    let header = adw::HeaderBar::new();

    let refresh_button = gtk::Button::builder()
        .icon_name("view-refresh-symbolic")
        .tooltip_text(gettext("Ask the server for its addresses again"))
        .build();
    let test_button = gtk::Button::builder()
        .icon_name("network-transmit-receive-symbolic")
        .tooltip_text(gettext("Measure the latency of every line"))
        .build();
    header.pack_end(&test_button);
    header.pack_end(&refresh_button);
    toolbar.add_top_bar(&header);

    let page = adw::PreferencesPage::new();

    let lines_group = adw::PreferencesGroup::builder()
        .title(gettext("Lines"))
        .description(gettext(
            "The current line is used until it stops answering; Tsukimi then \
             switches to another reachable line automatically.",
        ))
        .build();
    page.add(&lines_group);

    let actions_group = adw::PreferencesGroup::new();
    page.add(&actions_group);

    toolbar.set_content(Some(&page));
    overlay.set_child(Some(&toolbar));
    dialog.set_child(Some(&overlay));
    dialog.present(Some(parent));

    let labels: LatencyLabels = Rc::new(RefCell::new(Vec::new()));
    rebuild_rows(&lines_group, &labels, &overlay, &dialog);

    // --- automatically pick the fastest reachable line ---
    let auto_row = adw::ActionRow::builder()
        .title(gettext("Choose the fastest line"))
        .subtitle(gettext("Measures every line and clears the manual pin"))
        .build();
    auto_row.set_activatable(true);
    auto_row.add_prefix(&image("starred-symbolic"));
    auto_row.add_suffix(&image("go-next-symbolic"));
    {
        let overlay = overlay.clone();
        let lines_group = lines_group.clone();
        let labels = labels.clone();
        let dialog = dialog.clone();
        auto_row.connect_activated(move |_| {
            let overlay = overlay.clone();
            let lines_group = lines_group.clone();
            let labels = labels.clone();
            let dialog = dialog.clone();
            spawn(async move {
                let result =
                    spawn_tokio(async { JELLYFIN_CLIENT.use_automatic_line().await }).await;
                match result {
                    Ok(()) => {
                        toast(&overlay, gettext("Switched to the fastest line"));
                        rebuild_rows(&lines_group, &labels, &overlay, &dialog);
                    }
                    Err(e) => toast(&overlay, e.to_string()),
                }
            });
        });
    }
    actions_group.add(&auto_row);

    // --- measure everything ---
    {
        let overlay = overlay.clone();
        let labels = labels.clone();
        let button = test_button.clone();
        test_button.connect_clicked(move |_| {
            button.set_sensitive(false);
            let overlay = overlay.clone();
            let labels = labels.clone();
            let button = button.clone();
            spawn(async move {
                let statuses = spawn_tokio(async { JELLYFIN_CLIENT.measure_lines().await }).await;
                apply_latencies(&labels, &statuses);

                let reachable = statuses
                    .iter()
                    .filter(|line| line.latency_ms.is_some())
                    .count();
                toast(
                    &overlay,
                    format!(
                        "{} / {} {}",
                        reachable,
                        statuses.len(),
                        gettext("lines answered")
                    ),
                );
                button.set_sensitive(true);
            });
        });
    }

    // --- re-discover ---
    {
        let overlay = overlay.clone();
        let lines_group = lines_group.clone();
        let labels = labels.clone();
        let dialog = dialog.clone();
        refresh_button.connect_clicked(move |_| {
            let overlay = overlay.clone();
            let lines_group = lines_group.clone();
            let labels = labels.clone();
            let dialog = dialog.clone();
            spawn(async move {
                let before = JELLYFIN_CLIENT.line_statuses().len();
                let statuses = spawn_tokio(async { JELLYFIN_CLIENT.refresh_lines().await }).await;
                rebuild_rows(&lines_group, &labels, &overlay, &dialog);
                let added = statuses.len().saturating_sub(before);
                if added == 0 {
                    toast(&overlay, gettext("No new lines were reported"));
                } else {
                    toast(
                        &overlay,
                        format!("{} {}", added, gettext("new lines found")),
                    );
                }
            });
        });
    }
}

fn rebuild_rows(
    group: &adw::PreferencesGroup, labels: &LatencyLabels, overlay: &adw::ToastOverlay,
    dialog: &adw::Dialog,
) {
    while let Some(child) = group.first_child() {
        group.remove(&child);
    }
    labels.borrow_mut().clear();

    let statuses = JELLYFIN_CLIENT.line_statuses();
    if statuses.is_empty() {
        let row = adw::ActionRow::builder()
            .title(gettext("No lines known"))
            .subtitle(gettext(
                "Open the server once so its addresses can be discovered",
            ))
            .build();
        row.add_css_class("dim-label");
        group.add(&row);
        return;
    }

    for status in statuses {
        group.add(&build_row(status, labels, overlay, dialog));
    }
}

fn build_row(
    status: LineStatus, labels: &LatencyLabels, overlay: &adw::ToastOverlay, dialog: &adw::Dialog,
) -> adw::ActionRow {
    let row = adw::ActionRow::builder()
        .title(&status.label)
        .subtitle(&status.url)
        .build();

    let latency = gtk::Label::new(Some(&gettext("Not measured")));
    latency.add_css_class("dim-label");
    latency.add_css_class("caption");
    row.add_suffix(&latency);
    labels
        .borrow_mut()
        .push((status.url.clone(), latency.clone()));

    if status.current {
        row.add_css_class("accent");
        row.add_suffix(&image("object-select-symbolic"));
        row.set_tooltip_text(Some(&gettext("Line in use")));
    } else if status.preferred {
        row.add_prefix(&image("starred-symbolic"));
    }

    row.set_activatable(!status.current);
    {
        let url = status.url.clone();
        let overlay = overlay.clone();
        let dialog = dialog.clone();
        let row_clone = row.clone();
        row.connect_activated(move |_| {
            let url = url.clone();
            let overlay = overlay.clone();
            let dialog = dialog.clone();
            let row = row_clone.clone();
            // Show that something is happening before the probe completes.
            row.set_sensitive(false);
            spawn(async move {
                let result =
                    spawn_tokio(async move { JELLYFIN_CLIENT.switch_line(&url).await }).await;
                row.set_sensitive(true);
                match result {
                    Ok(()) => {
                        toast(&overlay, gettext("Line switched"));
                        dialog.force_close();
                    }
                    Err(e) => toast(&overlay, e.to_string()),
                }
            });
        });
    }

    row
}

fn apply_latencies(labels: &LatencyLabels, statuses: &[LineStatus]) {
    for (url, label) in labels.borrow().iter() {
        let text = match statuses.iter().find(|status| &status.url == url) {
            Some(status) => match status.latency_ms {
                Some(ms) => format!("{ms} ms"),
                None => gettext("Unreachable"),
            },
            None => continue,
        };
        label.set_text(&text);
    }
}

fn toast(overlay: &adw::ToastOverlay, message: impl Into<String>) {
    overlay.add_toast(
        adw::Toast::builder()
            .title(message.into())
            .timeout(3)
            .build(),
    );
}

fn image(icon: &str) -> gtk::Image {
    let image = gtk::Image::from_icon_name(icon);
    image.set_accessible_role(gtk::AccessibleRole::Presentation);
    image
}
