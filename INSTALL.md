# Installation Guide

This project relies on **Docker** (for containerized environment, services, and simulation) and **Just** (a modern command runner to execute project recipes).

## Requirements

- **Docker & Docker Compose plugin**
- **Just** (Command runner for the `Justfile`)

---

## 🐧 Linux Installation (Recommended)

### 1. Install Docker
For most Linux distributions (Ubuntu/Debian/Fedora), you can easily install Docker with the official installation script:

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
```

**Post-installation steps for Docker:**
To run docker commands without `sudo`, add your user to the docker group:
```bash
sudo usermod -aG docker $USER
newgrp docker
```

### 2. Install `just`
You can install `just` directly into your user's bin directory using their official script:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to ~/.local/bin
```
*(Ensure `~/.local/bin` is in your `PATH`)*

Alternatively, if you use a package manager:
- **Ubuntu/Debian (newer versions):** `sudo apt install just`
- **Arch Linux:** `sudo pacman -S just`
- **Homebrew (Linux/macOS):** `brew install just`

---

## 🪟 Windows Installation

**⚠️ IMPORTANT: Native Windows is not supported. You MUST use Windows Subsystem for Linux (WSL 2) to run this project.**

### 1. Install WSL 2
Open **PowerShell** as an Administrator and run:
```powershell
wsl --install
```
*Restart your computer if prompted.* This will install WSL 2 and default to an Ubuntu environment. Complete the Ubuntu setup by creating a UNIX username and password.

### 2. Install Docker Desktop
1. Download and install [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/).
2. During the installation, ensure the **"Use WSL 2 instead of Hyper-V"** box is checked.
3. Open Docker Desktop.
4. Go to **Settings (gear icon) -> Resources -> WSL Integration**.
5. Enable integration with your default WSL distro (e.g., Ubuntu).

### 3. Install `just` (Inside WSL)
Open your WSL terminal (e.g., search for "Ubuntu" in your Windows start menu) and install `just` using the Linux bash script:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to ~/.local/bin
```

Then, add it to your path so your terminal recognizes it:
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

---

## ✅ Verification
Open your terminal (Linux) or WSL terminal (Windows), navigate to the project directory, and run:

```bash
docker --version
just --version
just --list
```
If both installed correctly, `just --list` will parse the `Justfile` and show you the available commands to build, test, and run the thesis project.