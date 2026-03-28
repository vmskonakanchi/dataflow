import { 
  BarChart3, 
  ArrowRightLeft,
  Database,
  Cloud,
  Clock,
  Zap,
  Moon,
  Sun
} from "lucide-react";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarFooter
} from "@/components/ui/sidebar";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/components/theme-provider";

interface AppSidebarProps {
  activeView: 'dashboard' | 'sources' | 'sinks' | 'pipelines' | 'cronjobs';
  setActiveView: (view: any) => void;
}

export function AppSidebar({ activeView, setActiveView }: AppSidebarProps) {
  const { theme, setTheme } = useTheme();

  return (
    <Sidebar variant="inset" className="border-r-0 bg-background/50 backdrop-blur-xl">
      <SidebarHeader className="h-16 border-b border-border/50 flex items-center px-6">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-xl bg-gradient-to-br from-teal-400 to-blue-600 flex items-center justify-center shadow-lg shadow-teal-500/20">
            <Zap className="h-5 w-5 text-white fill-white" />
          </div>
          <span className="font-black text-xl tracking-tighter uppercase italic text-foreground">Dataflow</span>
        </div>
      </SidebarHeader>
      <SidebarContent className="p-4">
        <SidebarGroup>
          <SidebarGroupLabel className="px-2 pb-3 text-[10px] font-black uppercase tracking-widest text-muted-foreground opacity-50 text-teal-500 font-mono">Control Center</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu className="gap-1">
              {[
                { id: 'dashboard', label: 'Overview', icon: BarChart3 },
                { id: 'pipelines', label: 'Pipelines', icon: ArrowRightLeft },
                { id: 'sources', label: 'Sources', icon: Database },
                { id: 'sinks', label: 'Targets', icon: Cloud },
                { id: 'cronjobs', label: 'Schedules', icon: Clock },
              ].map((item) => (
                <SidebarMenuItem key={item.id}>
                  <SidebarMenuButton 
                    isActive={activeView === item.id}
                    onClick={() => setActiveView(item.id as any)}
                    className="h-11 px-4 rounded-xl transition-all duration-300 hover:bg-teal-500/5 group data-[active=true]:bg-teal-500/10 data-[active=true]:text-teal-400 data-[active=true]:shadow-[inset_0_0_0_1px_rgba(45,212,191,0.2)]"
                  >
                    <item.icon className="h-4 w-4 transition-transform group-hover:scale-110" />
                    <span className="font-bold tracking-tight">{item.label}</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="p-4 border-t border-border/50 mt-auto">
        <Button 
          variant="ghost" 
          size="icon" 
          onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
          className="w-full h-11 flex items-center justify-center gap-3 rounded-xl hover:bg-muted/50 transition-colors"
        >
          {theme === 'dark' ? <Sun className="h-4 w-4 text-amber-400" /> : <Moon className="h-4 w-4 text-slate-700" />}
          <span className="font-bold text-sm tracking-tight">{theme === 'dark' ? 'Light Mode' : 'Dark Mode'}</span>
        </Button>
      </SidebarFooter>
    </Sidebar>
  );
}
