# High-precision-Robotic-Surgery-System-
QNX-based real-time robotic teleoperation system for high-precision MedTech applications, featuring deterministic control, watchdog-based safety recovery, haptic feedback, and distributed communication between Raspberry Pi nodes.

Features
Real-time robotic arm control
Deterministic scheduling using QNX RTOS
Full-duplex TCP/IP communication
Watchdog-based fault recovery
Servo limit protection and safety enforcement
Haptic feedback alert system
Multi-threaded real-time architecture
Distributed dual Raspberry Pi system
SPI, I2C, GPIO, and Ethernet integration

System Architecture
Pi1 – Doctor Side
Joystick acquisition using SPI
Motion control generation
TCP communication
Haptic feedback handling
Pi2 – Robotic Arm Side
Servo control using PCA9685 over I2C
Watchdog monitoring
Safety and limit enforcement
Real-time robotic actuation
