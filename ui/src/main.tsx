import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './assets/index.css'
import App from './App'
import { TooltipProvider } from "@/components/ui/tooltip"
import { ThemeProvider } from "@/components/theme-provider"

createRoot(document.getElementById('root')!).render(
    <StrictMode>
        <ThemeProvider defaultTheme="system" storageKey="dataflow-theme">
            <TooltipProvider>
                <App />
            </TooltipProvider>
        </ThemeProvider>
    </StrictMode>,
)
