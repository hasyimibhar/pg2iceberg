import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Layout } from "@/components/layout";
import { PipelinesPage } from "@/pages/pipelines";
import { PipelineDetailPage } from "@/pages/pipeline-detail";
import { CreatePipelinePage } from "@/pages/create-pipeline";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<PipelinesPage />} />
          <Route path="/pipelines/new" element={<CreatePipelinePage />} />
          <Route path="/pipelines/:id" element={<PipelineDetailPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
